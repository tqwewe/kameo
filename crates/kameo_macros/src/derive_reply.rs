use quote::{quote, ToTokens};
use syn::{
    parse::{Parse, ParseStream},
    DeriveInput, Generics, Ident,
};

pub struct DeriveReply {
    ident: Ident,
    generics: Generics,
}

impl ToTokens for DeriveReply {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let Self { ident, generics } = self;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        tokens.extend(quote! {
            #[automatically_derived]
            impl #impl_generics ::kameo::Reply for #ident #ty_generics #where_clause {
                type Ok = Self;
                type Error = ();
                type Value = Self;

                #[inline]
                fn to_result(self) -> ::std::result::Result<Self, ::kameo::SendError<Msg, ()>> {
                    ::std::result::Result::Ok(self)
                }

                #[inline]
                fn into_boxed_err(self) -> ::std::option::Option<::std::boxed::Box<dyn ::std::fmt::Debug + ::std::marker::Send + ::std::marker::Sync + 'static>> {
                    ::std::option::Option::None
                }

                #[inline]
                fn into_value(self) -> Self::Value {
                    self
                }
            }
        });
    }
}

impl Parse for DeriveReply {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let input: DeriveInput = input.parse()?;
        let ident = input.ident;
        let generics = input.generics;

        Ok(DeriveReply { ident, generics })
    }
}
