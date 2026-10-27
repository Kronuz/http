#pragma once
#include <allocators/allocation.h>
namespace http::detail {
// The legacy preset borrows the process default when no owner is supplied.
inline allocators::AllocationContext stream_context(std::shared_ptr<std::pmr::memory_resource> owner = {}) noexcept {
    if (!owner) owner = std::shared_ptr<std::pmr::memory_resource>(
        std::shared_ptr<std::pmr::memory_resource>{}, std::pmr::get_default_resource());
    return allocators::AllocationContext(std::move(owner));
}
template <class T> class StreamAllocator : public allocators::OwnedAllocator<T> {
    using Base = allocators::OwnedAllocator<T>;
  public:
    explicit StreamAllocator(std::shared_ptr<std::pmr::memory_resource> owner = {}) noexcept
        : Base(stream_context(std::move(owner))) {}
    template <class U> StreamAllocator(const StreamAllocator<U> &other) noexcept : Base(other.context()) {}
    std::pmr::memory_resource *resource() const noexcept { return this->context().resource(); }
};
template <class T> using ResourceDelete = allocators::OwnedDelete<T>;
template <class T> using ResourceUnique = allocators::OwnedUnique<T>;
template <class T, class... Args>
ResourceUnique<T> make_resource_unique(std::shared_ptr<std::pmr::memory_resource> owner, Args &&...args) {
    return allocators::make_owned_unique<T>(stream_context(std::move(owner)), std::forward<Args>(args)...);
}
} // namespace http::detail
