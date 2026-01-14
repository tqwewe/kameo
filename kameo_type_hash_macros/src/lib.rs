//! Procedural macros for TypeHash derivation
//!
//! This crate provides the `#[derive(TypeHash)]` macro for automatically
//! implementing type hashes for actors and messages.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, GenericParam, Type};

/// Derive macro for TypeHash
///
/// This macro automatically generates a `HasTypeHash` implementation with
/// a compile-time computed hash based on the type name and generic parameters.
///
/// # Example
///
/// ```rust,ignore
/// #[derive(TypeHash)]
/// struct MyActor {
///     // fields...
/// }
///
/// #[derive(TypeHash)]
/// struct Cache<K, V> {
///     // fields...
/// }
/// ```
#[proc_macro_derive(TypeHash)]
pub fn derive_type_hash(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;
    let type_name_str = name.to_string();

    // Handle generics
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Generate the type string including generic parameters
    let type_string = if input.generics.params.is_empty() {
        type_name_str.clone()
    } else {
        let generic_names: Vec<String> = input
            .generics
            .params
            .iter()
            .map(|param| match param {
                GenericParam::Type(type_param) => type_param.ident.to_string(),
                GenericParam::Lifetime(lifetime) => lifetime.lifetime.to_string(),
                GenericParam::Const(const_param) => const_param.ident.to_string(),
            })
            .collect();

        format!("{}<{}>", type_name_str, generic_names.join(","))
    };

    let expanded = quote! {
        impl #impl_generics kameo::remote::type_hash::HasTypeHash for #name #ty_generics #where_clause {
            const TYPE_HASH: kameo::remote::type_hash::TypeHash =
                kameo::remote::type_hash::TypeHash::from_bytes(#type_string.as_bytes());
        }
    };

    TokenStream::from(expanded)
}

/// Attribute macro for custom type hash
///
/// This allows specifying a custom hash string instead of using the type name.
/// Useful for maintaining compatibility when renaming types.
///
/// # Example
///
/// ```rust,ignore
/// #[type_hash("MyOldActorName")]
/// struct MyNewActor {
///     // fields...
/// }
/// ```
#[proc_macro_attribute]
pub fn type_hash(args: TokenStream, input: TokenStream) -> TokenStream {
    let hash_str = parse_macro_input!(args as syn::LitStr).value();
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let expanded = quote! {
        #input

        impl #impl_generics kameo::remote::type_hash::HasTypeHash for #name #ty_generics #where_clause {
            const TYPE_HASH: kameo::remote::type_hash::TypeHash =
                kameo::remote::type_hash::TypeHash::from_bytes(#hash_str.as_bytes());
        }
    };

    TokenStream::from(expanded)
}

/// Macro for generating type hash for a specific type instantiation
///
/// This is useful for computing hashes of concrete generic types.
///
/// # Example
///
/// ```rust,ignore
/// const CACHE_HASH: TypeHash = type_hash_for!(Cache<String, User>);
/// ```
#[proc_macro]
pub fn type_hash_for(input: TokenStream) -> TokenStream {
    let ty = parse_macro_input!(input as Type);
    let type_string = quote!(#ty).to_string();

    let expanded = quote! {
        kameo::remote::type_hash::TypeHash::from_bytes(#type_string.as_bytes())
    };

    TokenStream::from(expanded)
}
