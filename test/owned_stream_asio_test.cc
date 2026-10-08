#include "http_asio.h"
#include <atomic>
#include <condition_variable>
#include <future>
#include <iostream>
#include <source_location>
#include <sstream>
using namespace http;
using namespace std::chrono_literals;
namespace {
void check(bool value, std::source_location where = std::source_location::current()) {
	if (!value)
		throw std::runtime_error("owned Asio stream invariant at " + std::to_string(where.line()));
}
struct Record {
	std::mutex mutex;
	std::condition_variable changed;
	unsigned writable = 0, settled = 0;
	ReplySettlement reason = ReplySettlement::Failed;
	std::weak_ptr<char> lease;
	bool retained = false;
};
struct Pending {
	OwnedStreamCapability reply;
	std::shared_ptr<Record> record;
};
struct App final : HttpHandler {
	std::mutex mutex;
	std::condition_variable ready;
	std::optional<Pending> pending;
	bool should_offload(const Request &) const override { return false; }
	void handle(const Request &request, ResponseWriter &writer) override {
		if (request.path == "/fast") {
			writer.send(200, "fast");
			return;
		}
		auto record = std::make_shared<Record>();
		auto lease = std::make_shared<char>();
		record->lease = lease;
		StreamOptions options;
		options.reply.deadline = std::chrono::steady_clock::now() +
								 (request.path == "/timeout" || request.path == "/blocked" ? 100ms : 5s);
		if (request.path == "/blocked" || request.path == "/shutdown")
			options.chunk_bytes = 8 * 1024 * 1024;
		options.reply.request_retention = lease;
		options.reply.on_settled = [record](ReplySettlement reason) {
			std::lock_guard lock(record->mutex);
			record->retained = !record->lease.expired();
			record->reason = reason;
			++record->settled;
			record->changed.notify_all();
		};
		options.on_writable = [record] {
			std::lock_guard lock(record->mutex);
			++record->writable;
			record->changed.notify_all();
		};
		auto reply = writer.stream(std::move(options));
		check(bool(reply));
		{
			std::lock_guard lock(mutex);
			check(!pending);
			pending.emplace(Pending{std::move(reply), record});
		}
		ready.notify_one();
	}
	Pending take() {
		std::unique_lock lock(mutex);
		check(ready.wait_for(lock, 2s, [&] { return pending.has_value(); }));
		auto result = std::move(*pending);
		pending.reset();
		return result;
	}
};
unsigned short port() {
	asio::io_context io;
	asio::ip::tcp::acceptor probe(io, {asio::ip::address_v4::loopback(), 0});
	return probe.local_endpoint().port();
}
struct Client {
	asio::io_context io;
	asio::ip::tcp::socket socket{io};
	asio::streambuf buffered;
	explicit Client(unsigned short port, bool small = false) {
		asio::error_code error;
		for (unsigned tries = 0; tries < 200; ++tries) {
			if (!socket.is_open())
				socket.open(asio::ip::tcp::v4());
			if (small)
				socket.set_option(asio::socket_base::receive_buffer_size(4096));
			socket.connect({asio::ip::address_v4::loopback(), port}, error);
			if (!error)
				return;
			socket.close();
			socket = asio::ip::tcp::socket(io);
			std::this_thread::sleep_for(5ms);
		}
		throw asio::system_error(error);
	}
	void request(std::string_view path, bool keep = false) {
		std::string text = "GET " + std::string(path) +
						   " HTTP/1.1\r\nHost: localhost\r\nConnection: " + (keep ? "keep-alive" : "close") +
						   "\r\n\r\n";
		asio::write(socket, asio::buffer(text));
	}
	std::string response() {
		asio::read_until(socket, buffered, "\r\n\r\n");
		std::istream input(&buffered);
		std::string line;
		std::getline(input, line);
		check(line.find("200") != std::string::npos);
		std::size_t length = 0;
		bool chunked = false;
		while (std::getline(input, line) && line != "\r") {
			if (line.starts_with("Content-Length: "))
				length = std::stoull(line.substr(16));
			if (line == "Transfer-Encoding: chunked\r")
				chunked = true;
		}
		if (chunked) {
			std::string body;
			while (true) {
				asio::read_until(socket, buffered, "\r\n");
				std::getline(input, line);
				const auto size = static_cast<std::size_t>(std::stoull(line, nullptr, 16));
				if (buffered.size() < size + 2)
					asio::read(socket, buffered, asio::transfer_exactly(size + 2 - buffered.size()));
				const auto offset = body.size();
				body.resize(offset + size);
				input.read(body.data() + offset, size);
				char ending[2];
				input.read(ending, 2);
				check(ending[0] == '\r' && ending[1] == '\n');
				if (!size)
					return body;
			}
		}
		if (buffered.size() < length)
			asio::read(socket, buffered, asio::transfer_exactly(length - buffered.size()));
		std::string body(length, '\0');
		input.read(body.data(), length);
		return body;
	}
};
void settled(const std::shared_ptr<Record> &record, ReplySettlement reason) {
	std::unique_lock lock(record->mutex);
	check(record->changed.wait_for(lock, 2s, [&] { return record->settled; }));
	check(record->settled == 1 && record->retained && record->reason == reason);
}
void transfer(App &app, unsigned short port, bool chunked = false) {
	Client client(port);
	client.request("/stream", true);
	auto pending = app.take();
	constexpr unsigned chunks = 32;
	check((chunked ? pending.reply->start_chunked({}, true)
				   : pending.reply->start({}, chunks * 65536, true)) == StreamSubmit::Accepted);
	auto receive = std::async(std::launch::async, [&] { return client.response(); });
	for (unsigned i = 0; i < chunks; ++i) {
		auto body = std::make_shared<const std::string>(65536, static_cast<char>('a' + i % 26));
		while (true) {
			unsigned before;
			{
				std::lock_guard lock(pending.record->mutex);
				before = pending.record->writable;
			}
			auto submitted = pending.reply->publish({body, *body});
			if (submitted == StreamSubmit::Accepted)
				break;
			check(submitted == StreamSubmit::Busy);
			std::unique_lock lock(pending.record->mutex);
			check(pending.record->changed.wait_for(
				lock, 2s, [&] { return pending.record->writable != before || pending.record->settled; }));
		}
	}
	check(pending.reply->finish() == StreamSubmit::Accepted);
	pending.reply.reset();
	auto body = receive.get();
	check(body.size() == chunks * 65536);
	for (unsigned i = 0; i < chunks; ++i)
		check(body.substr(i * 65536, 65536) == std::string(65536, static_cast<char>('a' + i % 26)));
	settled(pending.record, ReplySettlement::Written);
	client.request("/fast");
	check(client.response() == "fast");
}
void timeout(App &app, unsigned short port) {
	Client client(port);
	client.request("/timeout");
	auto pending = app.take();
	check(pending.reply->start({}, 1) == StreamSubmit::Accepted);
	settled(pending.record, ReplySettlement::TimedOut);
	check(pending.reply->finish() == StreamSubmit::Closed);
}
std::weak_ptr<const std::string> blocked(Pending &pending) {
	constexpr std::size_t bytes = 8 * 1024 * 1024;
	check(pending.reply->start({}, bytes) == StreamSubmit::Accepted);
	auto body = std::make_shared<const std::string>(bytes, 'x');
	std::weak_ptr<const std::string> alive = body;
	while (true) {
		unsigned before;
		{
			std::lock_guard lock(pending.record->mutex);
			before = pending.record->writable;
		}
		auto submitted = pending.reply->publish({body, *body});
		if (submitted == StreamSubmit::Accepted)
			break;
		check(submitted == StreamSubmit::Busy);
		std::unique_lock lock(pending.record->mutex);
		check(pending.record->changed.wait_for(lock, 2s, [&] { return pending.record->writable != before; }));
	}
	return alive;
}
void payload_retired(const std::weak_ptr<const std::string> &body) {
	const auto until = std::chrono::steady_clock::now() + 1s;
	while (!body.expired() && std::chrono::steady_clock::now() < until)
		std::this_thread::sleep_for(1ms);
	check(body.expired());
}
class TrackingResource final : public std::pmr::memory_resource {
  public:
	std::size_t used = 0;
	std::size_t allocations = 0, refuse_on = 0;

  private:
	void *do_allocate(std::size_t bytes, std::size_t alignment) override {
		if (++allocations == refuse_on)
			throw std::bad_alloc();
		auto *pointer = std::pmr::new_delete_resource()->allocate(bytes, alignment);
		used += bytes;
		return pointer;
	}
	void do_deallocate(void *pointer, std::size_t bytes, std::size_t alignment) override {
		used -= bytes;
		std::pmr::new_delete_resource()->deallocate(pointer, bytes, alignment);
	}
	bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override {
		return this == &other;
	}
};
void resource_lifetime() {
	asio::io_context io;
	auto resource = std::make_shared<TrackingResource>();
	StreamOptions options;
	options.resource = resource;
	options.reply.deadline = std::chrono::steady_clock::now() + 5s;
	auto control = detail::AsioOwnedStreamControl::create(io.get_executor(), std::move(options));
	const auto controls = resource->used;
	check(controls > 0);
	auto capability = detail::make_resource_unique<OwnedStreamReply>(resource, control->exchange);
	const auto capability_bytes = resource->used - controls;
	check(capability_bytes == sizeof(OwnedStreamReply));
	std::weak_ptr<detail::AsioOwnedStreamControl> weak_control = control;
	std::weak_ptr<detail::OwnedStreamExchange> weak_exchange = control->exchange;
	Response metadata;
	metadata.headers.emplace_back("X-Extra", std::string(2048, 'x'));
	const auto before_header = resource->used;
	resource->refuse_on = resource->allocations + 1;
	bool refused = false;
	try {
		capability->start(metadata, 0);
	} catch (const std::bad_alloc &) {
		refused = true;
	}
	check(refused && resource->used == before_header &&
		  control->exchange->phase() == detail::OwnedStreamExchange::Phase::Waiting);
	resource->refuse_on = 0;
	check(capability->start(metadata, 0) == StreamSubmit::Accepted);
	check(resource->used > controls);
	check(capability->finish() == StreamSubmit::Accepted);
	capability.reset();
	auto original = control->exchange->take_write();
	check(original.has_value());
	control->disable();
	control.reset();
	check(weak_control.expired() && !weak_exchange.expired());
	check(original->head.find(std::string(2048, 'x')) != std::string_view::npos);
	check(original->retention->complete_write(original->serial, ReplySettlement::Written));
	check(original->retention->settle());
	original.reset();
	check(weak_exchange.expired() && resource->used > 0);
	weak_control.reset();
	weak_exchange.reset();
	io.poll();
	check(resource->used == 0);
	std::cout << "owned stream PMR control requests=" << controls << " capability bytes=" << capability_bytes
			  << " writer bytes=" << sizeof(AsioResponseWriter) << '\n';
	for (std::size_t refused_allocation = 1; refused_allocation <= 2; ++refused_allocation) {
		resource->refuse_on = resource->allocations + refused_allocation;
		StreamOptions policy;
		policy.resource = resource;
		policy.reply.deadline = std::chrono::steady_clock::now() + 5s;
		refused = false;
		try {
			detail::AsioOwnedStreamControl::create(io.get_executor(), std::move(policy));
		} catch (const std::bad_alloc &) {
			refused = true;
		}
		check(refused && resource->used == 0);
	}
}
} // namespace
int main() {
	try {
		resource_lifetime();
		App app;
		HttpAsioService server(app, 1, 0, 64);
		const auto selected = port();
		server.start(selected);
		transfer(app, selected);
		transfer(app, selected, true);
		timeout(app, selected);
		Client slow(selected, true);
		slow.request("/blocked");
		auto timed = app.take();
		auto timed_body = blocked(timed);
		settled(timed.record, ReplySettlement::TimedOut);
		payload_retired(timed_body);
		Client abandoned(selected);
		abandoned.request("/stream");
		auto pending = app.take();
		pending.reply.reset();
		settled(pending.record, ReplySettlement::Abandoned);
		Client shutdown(selected, true);
		shutdown.request("/shutdown");
		auto closing = app.take();
		auto held_body = blocked(closing);
		const auto until = std::chrono::steady_clock::now() + 1s;
		while (shutdown.socket.available() < 512 && std::chrono::steady_clock::now() < until)
			std::this_thread::sleep_for(1ms);
		check(shutdown.socket.available() >= 512 && !held_body.expired());
		server.stop();
		settled(closing.record, ReplySettlement::Shutdown);
		payload_retired(held_body);
		std::cout << "multi-chunk transfer, producer backpressure, keep-alive, deadline, abandonment and "
					 "shutdown passed\n";
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
