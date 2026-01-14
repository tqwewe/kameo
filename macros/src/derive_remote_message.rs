use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    Attribute, Data, DeriveInput, Lit, Meta, MetaNameValue,
};

pub struct DeriveRemoteMessage {
    input: DeriveInput,
}

impl DeriveRemoteMessage {
    pub fn into_token_stream(self) -> TokenStream {
        let DeriveRemoteMessage { input } = self;
        let name = &input.ident;
        let generics = &input.generics;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        // Validate that this is a struct
        match &input.data {
            Data::Struct(_) => {}
            Data::Enum(_) => {
                return syn::Error::new_spanned(
                    name,
                    "RemoteMessage can only be derived for structs",
                )
                .to_compile_error();
            }
            Data::Union(_) => {
                return syn::Error::new_spanned(
                    name,
                    "RemoteMessage can only be derived for structs",
                )
                .to_compile_error();
            }
        }

        // Check for #[remote_name = "..."] attribute to override type name
        let custom_name = extract_remote_name(&input.attrs);

        // Generate the type name string - use custom name if provided, otherwise full name
        let type_name_str = if let Some(remote_name) = custom_name {
            quote! { #remote_name }
        } else if generics.params.is_empty() {
            quote! { stringify!(#name) }
        } else {
            quote! { stringify!(#name #ty_generics) }
        };

        quote! {
            #[automatically_derived]
            impl #impl_generics ::kameo::remote::type_hash::HasTypeHash for #name #ty_generics #where_clause {
                const TYPE_HASH: ::kameo::remote::type_hash::TypeHash = {
                    const TYPE_NAME: &str = #type_name_str;
                    ::kameo::remote::type_hash::TypeHash::from_bytes(TYPE_NAME.as_bytes())
                };
            }

            #[automatically_derived]
            impl #impl_generics ::kameo::remote::remote_message_trait::RemoteMessage for #name #ty_generics #where_clause {
                const TYPE_NAME: &'static str = #type_name_str;

                // Default implementation uses standard rkyv serialization
                // Individual message types can override for custom optimizations
            }
        }
    }
}

impl Parse for DeriveRemoteMessage {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(DeriveRemoteMessage {
            input: input.parse()?,
        })
    }
}

/// Extract the remote_name attribute value if present
/// Supports: #[remote_name = "CustomName"]
fn extract_remote_name(attrs: &[Attribute]) -> Option<String> {
    for attr in attrs {
        if attr.path().is_ident("remote_name") {
            if let Meta::NameValue(MetaNameValue {
                value:
                    syn::Expr::Lit(syn::ExprLit {
                        lit: Lit::Str(lit_str),
                        ..
                    }),
                ..
            }) = &attr.meta
            {
                return Some(lit_str.value());
            }
        }
    }
    None
}
