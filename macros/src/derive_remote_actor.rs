use quote::{ToTokens, quote};
use syn::{
    DeriveInput, Expr, ExprAssign, ExprLit, Generics, Ident, Lit, LitStr,
    parse::{Parse, ParseStream},
    spanned::Spanned,
};

pub struct DeriveRemoteActor {
    attrs: DeriveRemoteActorAttrs,
    generics: Generics,
    ident: Ident,
}

impl ToTokens for DeriveRemoteActor {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let Self {
            attrs,
            generics,
            ident,
        } = self;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let id = match &attrs.id {
            Some(id) => quote! { #id },
            None => quote! {
                ::std::concat!(::std::module_path!(), "::", ::std::stringify!(#ident))
            },
        };

        tokens.extend(quote! {
            #[automatically_derived]
            impl #impl_generics ::kameo::remote::RemoteActor for #ident #ty_generics #where_clause {
                const REMOTE_ID: &'static str = #id;
            }

            const _: () = {
                #[::kameo::remote::_internal::linkme::distributed_slice(
                    ::kameo::remote::_internal::REMOTE_ACTORS
                )]
                #[linkme(crate = ::kameo::remote::_internal::linkme)]
                static REG: (
                    &'static str,
                    ::kameo::remote::_internal::RemoteActorFns,
                ) = (
                    <#ident #ty_generics as ::kameo::remote::RemoteActor>::REMOTE_ID,
                    ::kameo::remote::_internal::RemoteActorFns {
                        link: (
                            |
                              actor_id: ::kameo::actor::ActorId,
                              sibling_id: ::kameo::actor::ActorId,
                              sibling_remote_id: ::std::borrow::Cow<'static, str>,
                            | {
                                ::std::boxed::Box::pin(::kameo::remote::_internal::link::<
                                    #ident #ty_generics,
                                >(
                                    actor_id,
                                    sibling_id,
                                    sibling_remote_id,
                                ))
                            }) as ::kameo::remote::_internal::RemoteLinkFn,
                        unlink: (
                            |
                              actor_id: ::kameo::actor::ActorId,
                              sibling_id: ::kameo::actor::ActorId,
                            | {
                                ::std::boxed::Box::pin(::kameo::remote::_internal::unlink::<
                                    #ident #ty_generics,
                                >(
                                    actor_id,
                                    sibling_id,
                                ))
                            }) as ::kameo::remote::_internal::RemoteUnlinkFn,
                        signal_link_died: (
                            |
                              dead_actor_id: ::kameo::actor::ActorId,
                              notified_actor_id: ::kameo::actor::ActorId,
                              stop_reason: kameo::error::ActorStopReason,
                            | {
                                ::std::boxed::Box::pin(::kameo::remote::_internal::signal_link_died::<
                                    #ident #ty_generics,
                                >(
                                    dead_actor_id,
                                    notified_actor_id,
                                    stop_reason,
                                ))
                            }) as ::kameo::remote::_internal::RemoteSignalLinkDiedFn,
                    },
                );
            };
        });
    }
}

impl Parse for DeriveRemoteActor {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let input: DeriveInput = input.parse()?;
        let mut attrs = None;
        for attr in input.attrs {
            if attr.path().is_ident("remote_actor") {
                if attrs.is_some() {
                    return Err(syn::Error::new(
                        attr.span(),
                        "remote_actor attribute already specified",
                    ));
                }
                attrs = Some(attr.parse_args_with(DeriveRemoteActorAttrs::parse)?);
            }
        }
        let ident = input.ident;

        Ok(DeriveRemoteActor {
            attrs: attrs.unwrap_or_default(),
            generics: input.generics,
            ident,
        })
    }
}

#[derive(Default)]
struct DeriveRemoteActorAttrs {
    id: Option<LitStr>,
}

impl Parse for DeriveRemoteActorAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let expr: ExprAssign = input
            .parse()
            .map_err(|_| syn::Error::new(input.span(), "expected id = \"...\" expression"))?;
        let Expr::Path(left_path) = expr.left.as_ref() else {
            return Err(syn::Error::new(expr.left.span(), "expected `id`"));
        };
        if !left_path.path.is_ident("id") {
            return Err(syn::Error::new(expr.left.span(), "expected `id`"));
        }
        let Expr::Lit(ExprLit {
            lit: Lit::Str(lit_str),
            ..
        }) = *expr.right
        else {
            return Err(syn::Error::new(
                expr.right.span(),
                "expected a string literal",
            ));
        };

        Ok(DeriveRemoteActorAttrs { id: Some(lit_str) })
    }
}
