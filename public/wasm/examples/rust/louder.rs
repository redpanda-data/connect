extern crate alloc;
extern crate core;
extern crate wee_alloc;

use alloc::vec::Vec;
use std::mem::MaybeUninit;
use std::slice;

/// Makes a message louder.
fn process(message: &String) -> String {
    return[&message, "!!!!111!!11!"].concat() ;
}

/// WebAssembly export that is called for each message being processed.
#[cfg_attr(all(target_arch = "wasm32"), export_name = "process")]
#[no_mangle]
pub unsafe extern "C" fn _process() {
    let ptr_size = _msg_as_bytes();

    let content_ptr = (ptr_size >> 32) as u32;
    let content_size = ptr_size as u32;
    let message = &ptr_to_string(content_ptr, content_size);

    let processed = process(&message);
    let (ptr, len) = string_to_ptr(&processed);
    std::mem::forget(processed);

    _msg_set_bytes(ptr, len);
}

//------------------------------------------------------------------------------

#[link(wasm_import_module = "benthos_wasm")]
extern "C" {
    /// WebAssembly import for mutating Benthos messages.
    ///
    /// Note: This is not an ownership transfer: Rust still owns the pointer
    /// and ensures it isn't deallocated during this call.
    #[link_name = "v0_msg_set_bytes"]
    fn _msg_set_bytes(ptr: u32, size: u32);

    /// WebAssembly import for accessing Benthos messages.
    ///
    /// Note: This is not an ownership transfer: Rust still owns the pointer
    /// and ensures it isn't deallocated during this call.
    #[link_name = "v0_msg_as_bytes"]
    fn _msg_as_bytes() -> u64;
}

//------------------------------------------------------------------------------

/// Returns a string from WebAssembly compatible numeric types representing
/// its pointer and length.
unsafe fn ptr_to_string(ptr: u32, len: u32) -> String {
    let slice = slice::from_raw_parts_mut(ptr as *mut u8, len as usize);
    let utf8 = std::str::from_utf8_unchecked_mut(slice);
    return String::from(utf8);
}

/// Returns a pointer and size pair for the given string in a way compatible
/// with WebAssembly numeric types.
///
/// Note: This doesn't change the ownership of the String. To intentionally
/// leak it, use [`std::mem::forget`] on the input after calling this.
unsafe fn string_to_ptr(s: &String) -> (u32, u32) {
    return (s.as_ptr() as u32, s.len() as u32);
}

/// Set the global allocator to the WebAssembly optimized one.
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

/// WebAssembly export that allocates a pointer (linear memory offset) that can
/// be used for a string.
///
/// This is an ownership transfer, which means the caller must call
/// [`deallocate`] when finished.
#[cfg_attr(all(target_arch = "wasm32"), export_name = "allocate")]
#[no_mangle]
pub extern "C" fn _allocate(size: u32) -> *mut u8 {
    allocate(size as usize)
}

/// Allocates size bytes and leaks the pointer where they start.
fn allocate(size: usize) -> *mut u8 {
    // Allocate the amount of bytes needed.
    let vec: Vec<MaybeUninit<u8>> = Vec::with_capacity(size);

    // into_raw leaks the memory to the caller.
    Box::into_raw(vec.into_boxed_slice()) as *mut u8
}


/// WebAssembly export that deallocates a pointer of the given size (linear
/// memory offset, byteCount) allocated by [`allocate`].
#[cfg_attr(all(target_arch = "wasm32"), export_name = "deallocate")]
#[no_mangle]
pub unsafe extern "C" fn _deallocate(ptr: u32, size: u32) {
    deallocate(ptr as *mut u8, size as usize);
}

/// Retakes the pointer which allows its memory to be freed.
unsafe fn deallocate(ptr: *mut u8, size: usize) {
    let _ = Vec::from_raw_parts(ptr, 0, size);
}