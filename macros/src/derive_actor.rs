use quote::{quote, ToTokens};
use syn::{
    custom_keyword, parenthesized,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    spanned::Spanned,
    token, DeriveInput, Generics, Ident, LitInt, LitStr, Token,
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

        let mailbox_expanded = match attrs.mailbox {
            MailboxKind::Bounded(_) => quote! {
                ::kameo::actor::BoundedMailbox<Self>
            },
            MailboxKind::Unbounded => quote! {
                ::kameo::actor::UnboundedMailbox<Self>
            },
        };
        let new_mailbox_expanded = match attrs.mailbox {
            MailboxKind::Bounded(cap) => {
                let cap = cap.unwrap_or(1000);
                quote! {
                    ::kameo::actor::BoundedMailbox::new(#cap)
                }
            }
            MailboxKind::Unbounded => quote! {
                ::kameo::actor::UnboundedMailbox::new()
            },
        };

        tokens.extend(quote! {
            #[automatically_derived]
            impl #impl_generics ::kameo::actor::Actor for #ident #ty_generics #where_clause {
                type Mailbox = #mailbox_expanded;

                fn name() -> &'static str {
                    #name
                }

                fn new_mailbox() -> (Self::Mailbox, ::kameo::actor::MailboxReceiver<Self>) {
                    #new_mailbox_expanded
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
    mailbox: MailboxKind,
}

impl Parse for DeriveActorAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        #[derive(Debug)]
        enum Attr {
            Name(name, LitStr),
            Mailbox(mailbox, MailboxKind),
        }
        let attrs: Punctuated<Attr, Token![,]> =
            Punctuated::parse_terminated_with(input, |input| {
                let lookahead = input.lookahead1();
                if lookahead.peek(name) {
                    let key: name = input.parse()?;
                    let _: Token![=] = input.parse()?;
                    let name: LitStr = input.parse()?;
                    Ok(Attr::Name(key, name))
                } else if lookahead.peek(mailbox) {
                    let key: mailbox = input.parse()?;
                    let _: Token![=] = input.parse()?;
                    let mailbox: MailboxKind = input.parse()?;
                    Ok(Attr::Mailbox(key, mailbox))
                } else {
                    Err(lookahead.error())
                }
            })?;

        let mut name = None;
        let mut mailbox = None;

        for attr in attrs {
            match attr {
                Attr::Name(key, s) => {
                    if name.is_none() {
                        name = Some(s);
                    } else {
                        return Err(syn::Error::new(key.span, "name already set"));
                    }
                }
                Attr::Mailbox(key, mb) => {
                    if mailbox.is_none() {
                        mailbox = Some(mb);
                    } else {
                        return Err(syn::Error::new(key.span, "mailbox already set"));
                    }
                }
            }
        }

        Ok(DeriveActorAttrs {
            name,
            mailbox: mailbox.unwrap_or_default(),
        })
    }
}

custom_keyword!(name);
custom_keyword!(mailbox);
custom_keyword!(bounded);
custom_keyword!(unbounded);

#[derive(Debug, Default)]
enum MailboxKind {
    Bounded(Option<usize>),
    #[default]
    Unbounded,
}

impl Parse for MailboxKind {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(bounded) {
            let _: bounded = input.parse()?;
            if input.peek(token::Paren) {
                // bounded(10)
                let content;
                parenthesized!(content in input);
                let cap_lit: LitInt = content.parse()?;
                let cap = cap_lit.base10_parse()?;
                if cap == 0 {
                    return Err(syn::Error::new(
                        cap_lit.span(),
                        "bounded mailbox channels requires capacity > 0",
                    ));
                }
                Ok(MailboxKind::Bounded(Some(cap)))
            } else {
                // bounded
                Ok(MailboxKind::Bounded(None))
            }
        } else if lookahead.peek(unbounded) {
            let _: unbounded = input.parse()?;
            Ok(MailboxKind::Unbounded)
        } else {
            Err(lookahead.error())
        }
    }
}
