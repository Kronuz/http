#pragma once
#include <limits>
#include <memory>
#include <memory_resource>
namespace http::detail {
// Carry the upstream resource owner through shared weak controls and escaped
// unique capabilities. Allocation refusal precedes construction and effects.
template <class T> class StreamAllocator {
  public:
	using value_type = T;
	explicit StreamAllocator(std::shared_ptr<std::pmr::memory_resource> owner = {})
		: owner_(std::move(owner)) {}
	template <class U> StreamAllocator(const StreamAllocator<U> &other) : owner_(other.owner()) {}
	T *allocate(std::size_t count) {
		if (count > std::numeric_limits<std::size_t>::max() / sizeof(T))
			throw std::bad_array_new_length();
		return static_cast<T *>(resource()->allocate(count * sizeof(T), alignof(T)));
	}
	void deallocate(T *pointer, std::size_t count) noexcept {
		resource()->deallocate(pointer, count * sizeof(T), alignof(T));
	}
	auto owner() const noexcept { return owner_; }
	template <class U> bool operator==(const StreamAllocator<U> &other) const noexcept {
		return resource() == other.resource();
	}
	std::pmr::memory_resource *resource() const noexcept {
		return owner_ ? owner_.get() : std::pmr::get_default_resource();
	}

  private:
	std::shared_ptr<std::pmr::memory_resource> owner_;
};
template <class T> struct ResourceDelete {
	std::shared_ptr<std::pmr::memory_resource> owner;
	void operator()(T *pointer) const noexcept {
		if (!pointer)
			return;
		std::destroy_at(pointer);
		auto *resource = owner ? owner.get() : std::pmr::get_default_resource();
		resource->deallocate(pointer, sizeof(T), alignof(T));
	}
};
template <class T> using ResourceUnique = std::unique_ptr<T, ResourceDelete<T>>;
template <class T, class... Args>
ResourceUnique<T> make_resource_unique(std::shared_ptr<std::pmr::memory_resource> owner, Args &&...args) {
	auto *resource = owner ? owner.get() : std::pmr::get_default_resource();
	auto *pointer = static_cast<T *>(resource->allocate(sizeof(T), alignof(T)));
	try {
		std::construct_at(pointer, std::forward<Args>(args)...);
	} catch (...) {
		resource->deallocate(pointer, sizeof(T), alignof(T));
		throw;
	}
	return ResourceUnique<T>(pointer, ResourceDelete<T>{std::move(owner)});
}
} // namespace http::detail
