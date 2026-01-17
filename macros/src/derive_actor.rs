use quote::{ToTokens, quote};
use syn::{
    DeriveInput, Generics, Ident, LitStr, Token, custom_keyword,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    spanned::Spanned,
};

pub struct DeriveActor {
    attrs: DeriveActorAttrs,
    ident: Ident,
    generics: Generics,
}

impl ToTokens for DeriveActor {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let Self {
            attrs,
            ident,
            generics,
        } = self;
        let name = match &attrs.name {
            Some(s) => s.value(),
            None => ident.to_string(),
        };
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        tokens.extend(quote! {
            #[automatically_derived]
            impl #impl_generics ::kameo::actor::Actor for #ident #ty_generics #where_clause {
                type Args = Self;
                type Error = ::kameo::error::Infallible;

                fn name() -> &'static str {
                    #name
                }

                async fn on_start(
                    state: Self::Args,
                    _actor_ref: ::kameo::actor::ActorRef<Self>,
                ) -> ::std::result::Result<Self, Self::Error> {
                    ::std::result::Result::Ok(state)
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
        let mut attrs = None;
        for attr in input.attrs {
            if attr.path().is_ident("actor") {
                if attrs.is_some() {
                    return Err(syn::Error::new(
                        attr.span(),
                        "actor attribute already specified",
                    ));
                }
                attrs = Some(attr.parse_args_with(DeriveActorAttrs::parse)?);
            }
        }

        Ok(DeriveActor {
            attrs: attrs.unwrap_or_default(),
            ident,
            generics,
        })
    }
}

#[derive(Default)]
struct DeriveActorAttrs {
    name: Option<LitStr>,
}

impl Parse for DeriveActorAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        #[derive(Debug)]
        enum Attr {
            Name(name, LitStr),
        }
        let attrs: Punctuated<Attr, Token![,]> =
            Punctuated::parse_terminated_with(input, |input| {
                let lookahead = input.lookahead1();
                if lookahead.peek(name) {
                    let key: name = input.parse()?;
                    let _: Token![=] = input.parse()?;
                    let name: LitStr = input.parse()?;
                    Ok(Attr::Name(key, name))
                } else {
                    Err(lookahead.error())
                }
            })?;

        let mut name = None;

        for attr in attrs {
            match attr {
                Attr::Name(key, s) => {
                    if name.is_none() {
                        name = Some(s);
                    } else {
                        return Err(syn::Error::new(key.span, "name already set"));
                    }
                }
            }
        }

        Ok(DeriveActorAttrs { name })
    }
}

custom_keyword!(name);
