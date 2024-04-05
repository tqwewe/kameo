use heck::ToUpperCamelCase;
use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote, quote_spanned, ToTokens};
use syn::{
    parse::{Parse, ParseStream, Parser},
    parse_quote, parse_quote_spanned,
    punctuated::Punctuated,
    spanned::Spanned,
    Attribute, Field, FnArg, Ident, ImplItem, ItemImpl, Meta, ReturnType, Signature, Token, Type,
    Visibility,
};

pub struct Actor {
    item_impl: ItemImpl,
    ident: Ident,
    messages: Vec<Message>,
    errors: Option<syn::Error>,
}

#[derive(Clone)]
struct Message {
    vis: Visibility,
    sig: Signature,
    ident: Ident,
    fields: Punctuated<Field, Token![,]>,
    msg_type: MessageType,
    attrs: Vec<TokenStream>,
}

#[derive(Clone, Copy)]
enum MessageType {
    Message,
    Query,
}

impl
    TryFrom<(
        Visibility,
        Signature,
        MessageType,
        Vec<TokenStream>,
        Vec<Vec<Attribute>>,
    )> for Message
{
    type Error = syn::Error;

    fn try_from(
        (vis, mut sig, msg_type, attrs, field_doc_attrs): (
            Visibility,
            Signature,
            MessageType,
            Vec<TokenStream>,
            Vec<Vec<Attribute>>,
        ),
    ) -> Result<Self, Self::Error> {
        let ident = format_ident!("{}", sig.ident.to_string().to_upper_camel_case());
        let fields: Punctuated<Field, Token![,]> = sig
            .inputs
            .iter_mut()
            .zip(field_doc_attrs)
            .filter_map(|(input, doc_attrs)| match input {
                FnArg::Receiver(_) => None,
                FnArg::Typed(pat_type) => Some((doc_attrs, pat_type)),
            })
            .map::<syn::Result<Field>, _>(|(doc_attrs, pat_type)| {
                let ident = match pat_type.pat.as_ref() {
                    syn::Pat::Ident(pat_ident) => pat_ident.ident.clone(),
                    _ => return Err(syn::Error::new(pat_type.span(), "unsupported pattern - argments must be named when used with the actor macro")),
                };
                let ty = &pat_type.ty;

                Ok(parse_quote! {
                    #( #doc_attrs )*
                    #vis #ident: #ty
                })
            })
            .collect::<Result<_, _>>()?;

        Ok(Message {
            vis,
            sig,
            ident,
            fields,
            msg_type,
            attrs,
        })
    }
}

impl Actor {
    fn extract_messages(item_impl: &mut ItemImpl) -> (Vec<Message>, Option<syn::Error>) {
        let mut errors = Vec::new();
        let messages = item_impl
            .items
            .iter_mut()
            .filter_map(|item| {
                let initial_error_count = errors.len();
                let message = match item {
                    ImplItem::Fn(impl_item_fn) => {
                        let mut attrs: Vec<_> = impl_item_fn.attrs.iter().filter(|attr| {
                            matches!(
                                &attr.meta,
                                Meta::NameValue(meta) if meta.path.segments.first().map(|seg| seg.ident == "doc").unwrap_or(false)
                            )
                        })
                            .map(|attr| quote! { #attr })
                            .collect();

                        let mut msg_type: Option<MessageType> = None;
                        impl_item_fn.attrs.retain(|attr| {
                            if msg_type.is_some() {
                                return true;
                            }
                            match &attr.meta {
                                Meta::Path(path) if path.segments.len() == 1 => {
                                    let first_segment =
                                        path.segments.first().unwrap().ident.to_string();
                                    if first_segment == "message" {
                                        msg_type = Some(MessageType::Message);
                                        false
                                    } else if first_segment == "query" {
                                        msg_type = Some(MessageType::Query);
                                        false
                                    } else {
                                        true
                                    }
                                }
                                Meta::List(list) if list.path.segments.len() == 1 => {
                                    let first_segment =
                                        list.path.segments.first().unwrap().ident.to_string();
                                    if first_segment == "message" {
                                        msg_type = Some(MessageType::Message);
                                    } else if first_segment == "query" {
                                        msg_type = Some(MessageType::Query);
                                    } else {
                                        return true
                                    }

                                    let args_res = Punctuated::<Meta, Token![,]>::parse_separated_nonempty.parse2(list.tokens.clone());
                                    match args_res {
                                        Ok(items) => {
                                            attrs.extend(items.into_iter().map(|attr| quote! { #[ #attr ] }));
                                        },
                                        Err(err) => {
                                            errors.push(err);
                                            return false;
                                        }
                                    }

                                    false
                                }
                                _ => true,
                            }
                        });

                        if let Some(msg_type) = msg_type {
                            for input in &impl_item_fn.sig.inputs {
                                if let FnArg::Typed(ty) = input {
                                    if let Err(err) = validate_param(&ty.ty) {
                                        errors.push(err);
                                    }
                                }
                            }

                            match impl_item_fn.sig.inputs.first() {
                                Some(FnArg::Receiver(recv))
                                    if matches!(msg_type, MessageType::Query)
                                        && recv.mutability.is_some() =>
                                {
                                    errors.push(syn::Error::new(
                                        recv.span(),
                                        "queries cannot take mutable references to self",
                                    ));
                                    return None;
                                }
                                Some(FnArg::Typed(_)) | None => {
                                    errors.push(syn::Error::new(
                                        impl_item_fn.sig.span(),
                                        "messages must take &mut self or &self",
                                    ));
                                    return None;
                                }
                                _ => {}
                            }

                            let field_doc_attrs: Vec<_> = impl_item_fn.sig.inputs.iter_mut().map(|input| {
                                match input {
                                    FnArg::Receiver(_) => vec![],
                                    FnArg::Typed(pat_type) => {
                                        let mut doc_attrs = Vec::new();
                                        let mut i = 0;
                                        while i < pat_type.attrs.len() {
                                            let is_doc_attr = matches!(
                                                &pat_type.attrs[i].meta,
                                                Meta::NameValue(meta) if meta.path.segments.first().map(|seg| seg.ident == "doc").unwrap_or(false)
                                            );
                                            if is_doc_attr {
                                                doc_attrs.push(pat_type.attrs.remove(i));
                                            } else {
                                                i += 1;
                                            }
                                        }

                                        doc_attrs
                                    },
                                }
                            }).collect();

                            match Message::try_from((
                                impl_item_fn.vis.clone(),
                                impl_item_fn.sig.clone(),
                                msg_type,
                                attrs,
                                field_doc_attrs,
                            )) {
                                Ok(message) => Some(message),
                                Err(err) => {
                                    errors.push(err);
                                    None
                                }
                            }
                        } else {
                            None
                        }
                    }
                    _ => None,
                };

                if errors.len() > initial_error_count {
                    None
                } else {
                    message
                }
            })
            .collect();

        let error = if !errors.is_empty() {
            let mut iter = errors.into_iter();
            let first = iter.next().unwrap();
            Some(iter.fold(first, |err, mut errors| {
                errors.combine(err);
                errors
            }))
        } else {
            None
        };

        (messages, error)
    }

    fn expand_msgs(&self) -> proc_macro2::TokenStream {
        let Self { messages, .. } = self;

        let msgs = messages.iter().map(
            |Message {
                 vis,
                 sig,
                 ident,
                 fields,
                 attrs,
                 ..
             }| {
                if fields.is_empty() {
                    quote! {
                        #( #attrs )*
                        #vis struct #ident;
                    }
                } else {
                    let generics = &sig.generics;
                    quote! {
                        #( #attrs )*
                        #vis struct #ident #generics {
                            #fields
                        }
                    }
                }
            },
        );

        quote! {
            #( #msgs )*
        }
    }

    fn expand_msg_impls(&self) -> proc_macro2::TokenStream {
        let Self {
            item_impl,
            ident: actor_ident,
            messages,
            ..
        } = self;
        let (_, actor_ty_generics, _) = item_impl.generics.split_for_impl();

        let msg_impls = messages.iter().map(
            |Message {
                 sig,
                 ident: msg_ident,
                 fields,
                 msg_type,
                 ..
             }| {
                let mut all_generics = item_impl.generics.clone();
                all_generics.params.extend(sig.generics.params.clone());
                if let Some(where_clause) = sig.generics.where_clause.clone() {
                    all_generics.make_where_clause().predicates.extend(where_clause.predicates);
                }
                let (_, msg_ty_generics, _) = sig.generics.split_for_impl();
                let (impl_generics, _, where_clause) = all_generics.split_for_impl();

                let trait_name = match msg_type {
                    MessageType::Message => quote_spanned! {sig.span()=> Message },
                    MessageType::Query => quote_spanned! {sig.span()=> Query },
                };
                let self_span = sig.inputs.first().and_then(|input|
                    if matches!(input, FnArg::Receiver(_)) {
                        Some(input.span())
                    } else {
                        None
                    }
                ).unwrap_or(Span::call_site());
                let state = match msg_type {
                    MessageType::Message => quote_spanned! {self_span=> state: &mut #actor_ident #actor_ty_generics },
                    MessageType::Query => quote_spanned! {self_span=> state: &#actor_ident #actor_ty_generics },
                };
                let msg = quote_spanned! {self_span=> msg: #msg_ident #msg_ty_generics };
                let fn_ident = &sig.ident;
                let reply = match sig.output.clone() {
                    ReturnType::Default => parse_quote_spanned! {sig.output.span()=>
                        ()
                    },
                    ReturnType::Type(_, ty) => ty,
                };
                let await_tokens = sig.asyncness.map(|_| quote_spanned! {sig.asyncness.span()=>
                    .await
                });

                let params = fields.iter().map(|field| {
                    let ident = &field.ident;
                    quote_spanned! {field.span()=>
                        msg.#ident
                    }
                });

                quote_spanned! {sig.span()=>
                    #[automatically_derived]
                    impl #impl_generics ::kameo::#trait_name<#actor_ident #actor_ty_generics> for #msg_ident #msg_ty_generics #where_clause {
                        type Reply = #reply;

                        async fn handle(#state, #[allow(unused_variables)] #msg) -> Self::Reply {
                            state.#fn_ident(#( #params ),*) #await_tokens
                        }
                    }
                }
            },
        );

        quote! {
            #( #msg_impls )*
        }
    }
}

impl ToTokens for Actor {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let item_impl = &self.item_impl;
        let msg_enum = self.expand_msgs();
        let msg_impl_message = self.expand_msg_impls();
        let errors = self.errors.clone().map(|err| err.into_compile_error());

        tokens.extend(quote! {
            #item_impl

            #msg_enum
            #msg_impl_message
            #errors
        });
    }
}

impl Parse for Actor {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut item_impl: ItemImpl = input.parse()?;

        let ident = match item_impl.self_ty.as_ref() {
            Type::Path(type_path) => type_path
                .path
                .segments
                .last()
                .as_ref()
                .ok_or_else(|| syn::Error::new(type_path.path.span(), "missing ident from path"))?
                .ident
                .clone(),
            _ => {
                return Err(syn::Error::new(
                    item_impl.self_ty.span(),
                    "expected a path or ident",
                ))
            }
        };
        let (messages, errors) = Actor::extract_messages(&mut item_impl);

        Ok(Actor {
            item_impl,
            ident,
            messages,
            errors,
        })
    }
}

fn validate_param(ty: &Type) -> syn::Result<()> {
    match ty {
        Type::ImplTrait(_) => Err(syn::Error::new(
            ty.span(),
            "impl trait types are not supported in actor messages",
        )),
        Type::Infer(_) => Err(syn::Error::new(
            ty.span(),
            "type cannot be inferred in actor messages",
        )),
        Type::Reference(_) => Err(syn::Error::new(
            ty.span(),
            "references cannot be used in messages",
        )),
        Type::Group(group) => validate_param(group.elem.as_ref()),
        Type::Paren(ty) => validate_param(&ty.elem),
        _ => Ok(()),
    }
}
