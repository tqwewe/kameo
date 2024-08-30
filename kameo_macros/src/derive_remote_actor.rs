use quote::{quote, ToTokens};
use syn::{
    parse::{Parse, ParseStream},
    DeriveInput, Ident,
};

pub struct DeriveRemoteActor {
    ident: Ident,
}

impl ToTokens for DeriveRemoteActor {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let Self { ident } = self;

        tokens.extend(quote! {
            #[automatically_derived]
            impl ::kameo::remote::RemoteActor for #ident {
                const REMOTE_ID: &'static str =
                    ::std::concat!(::std::module_path!(), "::", ::std::stringify!(#ident));
            }
        });
    }
}

impl Parse for DeriveRemoteActor {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let input: DeriveInput = input.parse()?;
        let ident = input.ident;

        Ok(DeriveRemoteActor { ident })
    }
}