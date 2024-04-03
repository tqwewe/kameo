use quote::{quote, ToTokens};
use syn::{
    parse::{Parse, ParseStream},
    DeriveInput, Ident,
};

pub struct DeriveActor {
    ident: Ident,
}

impl ToTokens for DeriveActor {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let ident = &self.ident;
        let name = ident.to_string();

        tokens.extend(quote! {
            #[automatically_derived]
            impl ::kameo::Actor for #ident {
                fn name() -> &'static str {
                    #name
                }
            }
        });
    }
}

impl Parse for DeriveActor {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let input: DeriveInput = input.parse()?;
        let ident = input.ident;

        Ok(DeriveActor { ident })
    }
}
