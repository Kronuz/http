#include "http_client_posix.h"
#include <charconv>
#include <iostream>

int main(int argc, char **argv) {
	try {
		if (argc != 4) throw std::invalid_argument("usage: http_client_get ADDRESS PORT TARGET");
		const std::string_view service(argv[2]);
		std::uint16_t port = 0;
		const auto parsed = std::from_chars(service.data(), service.data() + service.size(), port);
		if (parsed.ec != std::errc{} || parsed.ptr != service.data() + service.size() || !port)
			throw std::invalid_argument("invalid port");
		const http::client::Deadline deadline(std::chrono::seconds(2));
		std::string authority(argv[1]);
		if (authority.find(':') != std::string::npos) authority = "[" + authority + "]";
		authority += ":" + std::to_string(port);
		http::client::PreparedRequest request({"GET", authority, argv[3], {}, {}, true});
		const std::array endpoints{http::client::Endpoint::numeric(argv[1], port)};
		http::client::PosixConnection connection(endpoints, deadline);
		auto response = http::client::exchange(connection, std::move(request), deadline);
		std::cout << response.head.status << '\n' << response.body << '\n';
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
