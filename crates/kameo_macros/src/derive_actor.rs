use quote::{quote, ToTokens};
use syn::{
    parse::{Parse, ParseStream},
    DeriveInput, Generics, Ident,
};

pub struct DeriveActor {
    ident: Ident,
    generics: Generics,
}

impl ToTokens for DeriveActor {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let Self { ident, generics } = self;
        let name = ident.to_string();
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        tokens.extend(quote! {
            #[automatically_derived]
            impl #impl_generics ::kameo::actor::Actor for #ident #ty_generics #where_clause {
                type Mailbox = ::kameo::actor::UnboundedMailbox<Self>;

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
        let generics = input.generics;

        Ok(DeriveActor { ident, generics })
    }
}
