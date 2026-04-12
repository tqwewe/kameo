use darling::{FromMeta, ast::NestedMeta};
use heck::ToUpperCamelCase;
use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, format_ident, quote, quote_spanned};
use std::collections::{HashMap, HashSet};
use syn::{
    Attribute, Expr, Field, FnArg, GenericParam, Generics, Ident, ImplItem, ItemImpl, Meta,
    MetaNameValue, Pat, ReturnType, Signature, Token, Type, TypeParam, Visibility,
    parse::{Parse, ParseStream, Parser},
    parse_quote, parse_quote_spanned,
    punctuated::Punctuated,
    spanned::Spanned,
    visit_mut::{self, VisitMut},
};

#[derive(Debug, Default, FromMeta)]
pub struct MessagesArgs {
    messages: Option<Ident>,
    replies: Option<Ident>,
}

impl Parse for MessagesArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let tokens: TokenStream = input.parse()?;
        let attr_args = NestedMeta::parse_meta_list(tokens)
            .map_err(|e| syn::Error::new(Span::call_site(), e))?;
        Self::from_list(&attr_args).map_err(|e| syn::Error::new(Span::call_site(), e))
    }
}

pub struct Messages {
    args: MessagesArgs,
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
    attrs: Vec<TokenStream>,
    generics: Generics,
    ctx: Option<(Ident, usize)>,
}

impl
    TryFrom<(
        Visibility,
        Signature,
        Vec<TokenStream>,
        Vec<Vec<Attribute>>,
        Generics,
        Option<Ident>,
    )> for Message
{
    type Error = syn::Error;

    fn try_from(
        (vis, mut sig, attrs, field_doc_attrs, generics, ctx): (
            Visibility,
            Signature,
            Vec<TokenStream>,
            Vec<Vec<Attribute>>,
            Generics,
            Option<Ident>,
        ),
    ) -> Result<Self, Self::Error> {
        let ident = format_ident!("{}", sig.ident.to_string().to_upper_camel_case());
        let mut ctx_pos = None;
        let fields: Punctuated<Field, Token![,]> = sig
            .inputs
            .iter_mut()
            .zip(field_doc_attrs)
            .enumerate()
            .filter_map(|(i, (input, doc_attrs))| match input {
                FnArg::Receiver(_) => None,
                FnArg::Typed(pat_type) => {
                    if let Some(ctx) = &ctx {
                        if let Pat::Ident(pat_ident) = &*pat_type.pat {
                            if &pat_ident.ident == ctx {
                                ctx_pos = Some(i.saturating_sub(1));
                                return None;
                            }
                        }
                    }

                    Some((doc_attrs, pat_type))
                },
            })
            .map::<syn::Result<Field>, _>(|(doc_attrs, pat_type)| {
                let ident = match pat_type.pat.as_ref() {
                    syn::Pat::Ident(pat_ident) => pat_ident.ident.clone(),
                    _ => return Err(syn::Error::new(pat_type.span(), "unsupported pattern - arguments must be named when used with the actor macro")),
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
            attrs,
            generics,
            ctx: ctx.zip(ctx_pos),
        })
    }
}

impl Messages {
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
                                Meta::NameValue(meta) if meta.path.segments.first().is_some_and(|seg| seg.ident == "doc")
                            )
                        })
                            .map(|attr| quote! { #attr })
                            .collect();

                        let mut is_message = false;
                        let mut ctx = None;
                        impl_item_fn.attrs.retain(|attr| {
                            if is_message {
                                return true;
                            }
                            match &attr.meta {
                                Meta::Path(path) if path.segments.len() == 1 => {
                                    let first_segment =
                                        path.segments.first().unwrap().ident.to_string();
                                    if first_segment == "message" {
                                        is_message = true;
                                        false
                                    } else {
                                        true
                                    }
                                }
                                Meta::List(list) if list.path.segments.len() == 1 => {
                                    let first_segment =
                                        list.path.segments.first().unwrap().ident.to_string();
                                    if first_segment == "message" {
                                        is_message = true;
                                    } else {
                                        return true;
                                    }

                                    let args_res = Punctuated::<Meta, Token![,]>::parse_separated_nonempty.parse2(list.tokens.clone());
                                    match args_res {
                                        Ok(items) => {
                                            attrs.extend(items.into_iter().filter_map(|attr| {
                                                if ctx.is_none() {
                                                    match attr {
                                                        Meta::Path(path) if path.is_ident("ctx") => {
                                                            ctx = Some(Ident::new("ctx", Span::call_site()));
                                                            return None;
                                                        },
                                                        Meta::NameValue(MetaNameValue { path, value, .. }) if path.is_ident("ctx") => {
                                                            match value {
                                                                Expr::Path(path) => {
                                                                    match path.path.require_ident() {
                                                                        Ok(ident) => {
                                                                            ctx = Some(ident.clone());
                                                                        },
                                                                        Err(err) => {
                                                                            errors.push(err);
                                                                        },
                                                                    }
                                                                },
                                                                _ => {
                                                                    errors.push(syn::Error::new(value.span(), "expected ctx attr to be an ident"));
                                                                },
                                                            }
                                                            return None;
                                                        },
                                                        _ => {}
                                                    }
                                                }

                                                Some(quote! { #[ #attr ] })
                                            }));
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

                        if !is_message {
                            return None;
                        }

                        impl_item_fn.attrs.push(parse_quote! ( #[allow(clippy::unused_self, reason = "self required for message handlers")] ));
                        impl_item_fn.attrs.push(parse_quote! ( #[allow(clippy::needless_pass_by_value, reason = "references are not allowed in message handlers")] ));

                        let mut generics = vec![];
                        let impl_item_generics: Vec<_> = item_impl.generics
                            .lifetimes()
                            .filter(|lifetime| !impl_item_fn.sig.generics.lifetimes().any(|lt| lt == *lifetime))
                            .cloned()
                            .map(GenericParam::Lifetime)
                            .chain(
                                item_impl.generics
                                    .type_params()
                                    .filter(|type_param| !impl_item_fn.sig.generics.type_params().any(|tp| tp == *type_param))
                                    .cloned()
                                    .map(GenericParam::Type)
                            ).collect();
                        for input in &impl_item_fn.sig.inputs {
                            if let FnArg::Typed(ty) = input {
                                if let Some(ctx) = &ctx {
                                    if let Pat::Ident(pat_ident) = &*ty.pat {
                                        if &pat_ident.ident == ctx {
                                            continue;
                                        }
                                    }
                                }

                                if let Err(err) = validate_param(&ty.ty) {
                                    errors.push(err);
                                }

                                generics.extend(contains_generic_in_param(&ty.ty, &impl_item_generics));
                            }
                        }
                        if let ReturnType::Type(_, ty) = &impl_item_fn.sig.output {
                            generics.extend(contains_generic_in_param(ty, &impl_item_generics));
                        }

                        generics.dedup();
                        let generics = if generics.is_empty() {
                            impl_item_fn.sig.generics.clone()
                        } else {
                            let lifetimes = generics
                                .iter()
                                .filter(|param| matches!(param, GenericParam::Lifetime(_)))
                                .cloned()
                                .chain(
                                    impl_item_fn.sig.generics.lifetimes().cloned().map(GenericParam::Lifetime)
                                );
                            let types = generics
                                .iter()
                                .filter(|param| matches!(param, GenericParam::Type(_)))
                                .cloned()
                                .chain(
                                    impl_item_fn.sig.generics.type_params().cloned().map(GenericParam::Type)
                                );
                            parse_quote! { <#( #lifetimes ),* #( #types, )*> }
                        };

                        match impl_item_fn.sig.inputs.first() {
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
                                            Meta::NameValue(meta) if meta.path.segments.first().is_some_and(|seg| seg.ident == "doc")
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
                            attrs,
                            field_doc_attrs,
                            generics,
                            ctx,
                        )) {
                            Ok(message) => Some(message),
                            Err(err) => {
                                errors.push(err);
                                None
                            }
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

        let error = if errors.is_empty() {
            None
        } else {
            let mut iter = errors.into_iter();
            let first = iter.next().unwrap();
            Some(iter.fold(first, |err, mut errors| {
                errors.combine(err);
                errors
            }))
        };

        (messages, error)
    }

    fn expand_msgs(&self) -> proc_macro2::TokenStream {
        let Self { messages, .. } = self;

        let msgs = messages.iter().map(
            |Message {
                 vis,
                 ident,
                 fields,
                 attrs,
                 generics,
                 ..
             }| {
                if fields.is_empty() {
                    quote! {
                        #( #attrs )*
                        #vis struct #ident;
                    }
                } else {
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
                 generics,
                 ctx,
                 ..
             }| {
                let mut all_generics = item_impl.generics.clone();
                all_generics.params.extend(sig.generics.params.clone());
                if let Some(where_clause) = sig.generics.where_clause.clone() {
                    all_generics.make_where_clause().predicates.extend(where_clause.predicates);
                }
                let (_, msg_ty_generics, _) = generics.split_for_impl();
                let (impl_generics, _, where_clause) = all_generics.split_for_impl();

                let trait_name = quote_spanned! {sig.span()=> Message };
                let self_span = sig.inputs.first().and_then(|input|
                    if matches!(input, FnArg::Receiver(_)) {
                        Some(input.span())
                    } else {
                        None
                    }
                ).unwrap_or(Span::call_site());
                let self_ref = quote! { &mut self };
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

                let mut params: Vec<_> = fields.iter().map(|field| {
                    let ident = &field.ident;
                    quote_spanned! {field.span()=>
                        msg.#ident
                    }
                }).collect();

                let ctx_ident = if let Some((ctx, i)) = ctx {
                    params.insert(*i, quote! { #ctx });
                    quote_spanned! {ctx.span()=>
                        #ctx
                    }
                } else {
                    quote! { _ctx }
                };

                quote_spanned! {sig.span()=>
                    #[automatically_derived]
                    impl #impl_generics ::kameo::message::#trait_name<#msg_ident #msg_ty_generics> for #actor_ident #actor_ty_generics #where_clause {
                        type Reply = #reply;

                        async fn handle(#self_ref, #[allow(unused_variables)] #msg, #ctx_ident: &mut ::kameo::message::Context<Self, Self::Reply>) -> Self::Reply {
                            self.#fn_ident(#( #params ),*) #await_tokens
                        }
                    }
                }
            },
        );

        quote! {
            #( #msg_impls )*
        }
    }

    fn expand_msg_enum(&self, name: syn::Ident) -> proc_macro2::TokenStream {
        let vis = match self.message_enum_visibility() {
            Ok(vis) => vis,
            Err(err) => return err.into_compile_error(),
        };

        let mut builder = EnumBuilder::new();

        let payload_messages: Vec<_> = self
            .messages
            .iter()
            .filter(|m| !m.fields.is_empty())
            .collect();

        for type_param in self.item_impl.generics.type_params() {
            let is_used = payload_messages.iter().any(|m| {
                m.generics
                    .type_params()
                    .any(|tp| tp.ident == type_param.ident)
            });
            if is_used {
                builder.register_impl_param(type_param);
            }
        }

        let variants: Vec<_> = self
            .messages
            .iter()
            .map(|message| {
                let Message {
                    sig,
                    ident: msg_ident,
                    fields,
                    ..
                } = message;

                if fields.is_empty() {
                    return quote! { #msg_ident };
                }

                let renames = builder.register_fn_params(sig, |_| true, |_, _| true);

                let type_args: Vec<_> = message
                    .generics
                    .type_params()
                    .map(|tp| {
                        renames
                            .get(&tp.ident.to_string())
                            .cloned()
                            .unwrap_or_else(|| tp.ident.clone())
                    })
                    .collect();

                if type_args.is_empty() {
                    quote! { #msg_ident(#msg_ident) }
                } else {
                    quote! { #msg_ident(#msg_ident<#( #type_args ),*>) }
                }
            })
            .collect();

        builder.finish(vis, name, variants)
    }

    fn expand_response_enum(&self, name: syn::Ident) -> proc_macro2::TokenStream {
        let vis = match self.message_enum_visibility() {
            Ok(vis) => vis,
            Err(err) => return err.into_compile_error(),
        };

        let mut builder = EnumBuilder::new();

        let payload_messages: Vec<_> = self
            .messages
            .iter()
            .filter(|m| non_unit_return(&m.sig.output).is_some())
            .collect();

        for type_param in self.item_impl.generics.type_params() {
            let is_used = payload_messages.iter().any(|m| {
                if let ReturnType::Type(_, ty) = &m.sig.output {
                    !contains_generic_in_param(ty, &[GenericParam::Type(type_param.clone())])
                        .is_empty()
                } else {
                    false
                }
            });
            if is_used {
                builder.register_impl_param(type_param);
            }
        }

        let variants: Vec<_> = self
            .messages
            .iter()
            .map(|message| {
                let Message {
                    sig,
                    ident: msg_ident,
                    ..
                } = message;

                let Some(return_ty) = non_unit_return(&sig.output) else {
                    return quote! { #msg_ident };
                };

                let renames = builder.register_fn_params(
                    sig,
                    |tp| {
                        !contains_generic_in_param(
                            return_ty,
                            &[GenericParam::Type(tp.clone())],
                        )
                        .is_empty()
                    },
                    |pred, renames| {
                        if let syn::WherePredicate::Type(pred_type) = pred {
                            if let Type::Path(type_path) = &pred_type.bounded_ty {
                                if type_path
                                    .path
                                    .get_ident()
                                    .is_some_and(|id| !renames.contains_key(&id.to_string()))
                                {
                                    return false;
                                }
                            }
                        }
                        true
                    },
                );

                let mut return_ty = return_ty.clone();
                RenameTypeParams { renames: &renames }.visit_type_mut(&mut return_ty);

                quote! { #msg_ident(#return_ty) }
            })
            .collect();

        builder.finish(vis, name, variants)
    }

    fn message_enum_visibility(&self) -> syn::Result<Visibility> {
        let Some(first) = self.messages.first() else {
            return Ok(Visibility::Inherited);
        };
        let first_visibility = first.vis.to_token_stream().to_string();

        for message in self.messages.iter().skip(1) {
            if message.vis.to_token_stream().to_string() != first_visibility {
                return Err(syn::Error::new(
                    message.sig.ident.span(),
                    "message enum requires all message handlers to have the same visibility",
                ));
            }
        }

        Ok(first.vis.clone())
    }

    fn expand_dispatch_impl(
        &self,
        msg_enum_name: &Ident,
        response_enum_name: &Ident,
    ) -> proc_macro2::TokenStream {
        let Self {
            item_impl,
            ident: actor_ident,
            messages,
            ..
        } = self;
        let (impl_generics, actor_ty_generics, where_clause) =
            item_impl.generics.split_for_impl();

        // Each message contributes one Message<MsgType> bound on the actor.
        let msg_bounds = messages.iter().map(|m| {
            let msg_ident = &m.ident;
            let (_, msg_ty_generics, _) = m.generics.split_for_impl();
            quote! { #actor_ident #actor_ty_generics: ::kameo::message::Message<#msg_ident #msg_ty_generics> }
        });

        let match_arms = messages.iter().map(|message| {
            let Message {
                sig,
                ident: msg_ident,
                fields,
                ..
            } = message;

            // Pattern: Enum::Variant or Enum::Variant(msg)
            let (pattern, ask_arg, map_msg_fn) = if fields.is_empty() {
                (
                    quote! { #msg_enum_name::#msg_ident },
                    quote! { #msg_ident },
                    quote! { |_| #msg_enum_name::#msg_ident },
                )
            } else {
                (
                    quote! { #msg_enum_name::#msg_ident(msg) },
                    quote! { msg },
                    quote! { #msg_enum_name::#msg_ident },
                )
            };

            // Capture reply and build response variant.
            let (capture, response_variant) = if non_unit_return(&sig.output).is_some() {
                (
                    quote! { let reply = },
                    quote! { #response_enum_name::#msg_ident(reply) },
                )
            } else {
                (quote! {}, quote! { #response_enum_name::#msg_ident })
            };

            quote! {
                #pattern => {
                    #capture self.ask(#ask_arg).send().await
                        .map_err(|e| e.map_msg(#map_msg_fn))?;
                    ::std::result::Result::Ok(#response_variant)
                }
            }
        });

        quote! {
            #[automatically_derived]
            #[allow(async_fn_in_trait)]
            impl #impl_generics ::kameo::message::Dispatch<#msg_enum_name #actor_ty_generics>
            for ::kameo::actor::ActorRef<#actor_ident #actor_ty_generics>
            #where_clause
            where
                #( #msg_bounds, )*
            {
                type Response = #response_enum_name #actor_ty_generics;

                async fn dispatch(
                    &self,
                    msg: #msg_enum_name #actor_ty_generics,
                ) -> ::std::result::Result<Self::Response, ::kameo::error::SendError<#msg_enum_name #actor_ty_generics>> {
                    match msg {
                        #( #match_arms )*
                    }
                }
            }
        }
    }
}

impl ToTokens for Messages {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let item_impl = &self.item_impl;
        let msg_structs = self.expand_msgs();
        let msg_enum = self
            .args
            .messages
            .clone()
            .map(|name| self.expand_msg_enum(name));
        let msg_impl_message = self.expand_msg_impls();
        let response_enum = self
            .args
            .replies
            .clone()
            .map(|name| self.expand_response_enum(name));
        let dispatch_impl = self
            .args
            .messages
            .as_ref()
            .zip(self.args.replies.as_ref())
            .map(|(msg_name, resp_name)| self.expand_dispatch_impl(msg_name, resp_name));
        let errors = self.errors.clone().map(syn::Error::into_compile_error);

        tokens.extend(quote! {
            #item_impl

            #msg_structs
            #msg_enum
            #msg_impl_message
            #errors
            #response_enum
            #dispatch_impl
        });
    }
}

struct RegisteredEnumTypeParam {
    original: String,
    ident: Ident,
    key: String,
}

struct EnumBuilder {
    generics: Generics,
    registered: Vec<RegisteredEnumTypeParam>,
    used_names: HashSet<String>,
    where_predicates: HashSet<String>,
}

impl EnumBuilder {
    fn new() -> Self {
        Self {
            generics: Generics::default(),
            registered: Vec::new(),
            used_names: HashSet::new(),
            where_predicates: HashSet::new(),
        }
    }

    fn register_impl_param(&mut self, type_param: &TypeParam) {
        self.used_names.insert(type_param.ident.to_string());
        self.registered.push(RegisteredEnumTypeParam {
            original: type_param.ident.to_string(),
            ident: type_param.ident.clone(),
            key: type_param_key(type_param, None),
        });
        self.generics.params.push(GenericParam::Type(type_param.clone()));
    }

    fn register_fn_params(
        &mut self,
        sig: &Signature,
        param_filter: impl Fn(&TypeParam) -> bool,
        predicate_filter: impl Fn(&syn::WherePredicate, &HashMap<String, Ident>) -> bool,
    ) -> HashMap<String, Ident> {
        let mut renames = HashMap::new();
        let mut params_to_add = Vec::new();

        for type_param in sig.generics.type_params() {
            if !param_filter(type_param) {
                continue;
            }

            let original = type_param.ident.to_string();
            let key = type_param_key(type_param, sig.generics.where_clause.as_ref());

            if let Some(existing) = self
                .registered
                .iter()
                .find(|r| r.original == original && r.key == key)
            {
                renames.insert(original, existing.ident.clone());
                continue;
            }

            let ident = unique_type_param_ident(&type_param.ident, &mut self.used_names);
            self.registered.push(RegisteredEnumTypeParam {
                original: original.clone(),
                ident: ident.clone(),
                key,
            });
            renames.insert(original, ident.clone());
            params_to_add.push((type_param.clone(), ident));
        }

        for (mut type_param, ident) in params_to_add {
            type_param.ident = ident;
            RenameTypeParams { renames: &renames }.visit_type_param_mut(&mut type_param);
            self.generics.params.push(GenericParam::Type(type_param));
        }

        if let Some(where_clause) = &sig.generics.where_clause {
            for predicate in &where_clause.predicates {
                if !predicate_filter(predicate, &renames) {
                    continue;
                }
                let mut predicate = predicate.clone();
                RenameTypeParams { renames: &renames }
                    .visit_where_predicate_mut(&mut predicate);
                let key = predicate.to_token_stream().to_string();
                if self.where_predicates.insert(key) {
                    self.generics.make_where_clause().predicates.push(predicate);
                }
            }
        }

        renames
    }

    fn finish(self, vis: Visibility, name: Ident, variants: Vec<TokenStream>) -> TokenStream {
        let where_clause = &self.generics.where_clause;
        let generics = &self.generics;
        quote! {
            #vis enum #name #generics #where_clause {
                #( #variants, )*
            }
        }
    }
}

fn type_param_key(type_param: &TypeParam, where_clause: Option<&syn::WhereClause>) -> String {
    let mut tokens = type_param.to_token_stream().to_string();
    if let Some(where_clause) = where_clause {
        tokens.push_str(" where ");
        tokens.push_str(&where_clause.predicates.to_token_stream().to_string());
    }
    tokens
}

fn unique_type_param_ident(ident: &Ident, used_names: &mut HashSet<String>) -> Ident {
    let name = ident.to_string();
    if used_names.insert(name.clone()) {
        return ident.clone();
    }

    for i in 2usize.. {
        let candidate = format_ident!("{}{}", name, i, span = ident.span());
        if used_names.insert(candidate.to_string()) {
            return candidate;
        }
    }

    unreachable!("unbounded suffix search must find an unused type parameter name")
}

struct RenameTypeParams<'a> {
    renames: &'a HashMap<String, Ident>,
}

impl VisitMut for RenameTypeParams<'_> {
    fn visit_path_mut(&mut self, path: &mut syn::Path) {
        if path.leading_colon.is_none() && path.segments.len() == 1 {
            if let Some(segment) = path.segments.first_mut() {
                if let Some(rename) = self.renames.get(&segment.ident.to_string()) {
                    segment.ident = rename.clone();
                }
            }
        }

        visit_mut::visit_path_mut(self, path);
    }
}

impl Messages {
    pub fn parse(input: TokenStream, args: MessagesArgs) -> syn::Result<Self> {
        let mut item_impl: ItemImpl = syn::parse2(input)?;

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
                ));
            }
        };
        let (messages, errors) = Messages::extract_messages(&mut item_impl);

        Ok(Messages {
            args,
            item_impl,
            ident,
            messages,
            errors,
        })
    }
}

fn non_unit_return(output: &ReturnType) -> Option<&Type> {
    match output {
        ReturnType::Default => None,
        ReturnType::Type(_, ty)
            if matches!(ty.as_ref(), Type::Tuple(t) if t.elems.is_empty()) =>
        {
            None
        }
        ReturnType::Type(_, ty) => Some(ty),
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

fn contains_generic_in_param(ty: &Type, generics: &[GenericParam]) -> Vec<GenericParam> {
    match ty {
        Type::Array(array) => contains_generic_in_param(&array.elem, generics),
        Type::BareFn(bare_fn) => {
            let mut params: Vec<_> = bare_fn
                .inputs
                .iter()
                .flat_map(|input| contains_generic_in_param(&input.ty, generics))
                .collect();
            if let ReturnType::Type(_, ty) = &bare_fn.output {
                params.extend(contains_generic_in_param(ty, generics));
            }
            params
        }
        Type::Group(group) => contains_generic_in_param(&group.elem, generics),
        Type::Paren(paren) => contains_generic_in_param(&paren.elem, generics),
        Type::Path(path) => {
            if let Some(ident) = path.path.get_ident() {
                let is_in_generics = generics
                    .iter()
                    .filter_map(|param| match param {
                        GenericParam::Type(type_param) => Some(type_param),
                        _ => None,
                    })
                    .any(|type_param| &type_param.ident == ident);
                if is_in_generics {
                    return vec![parse_quote! { #ident }];
                }
            }

            vec![]
        }
        Type::Ptr(ptr) => contains_generic_in_param(&ptr.elem, generics),
        Type::Reference(reference) => {
            let mut params = Vec::new();
            if let Some(lifetime) = &reference.lifetime {
                let is_in_generics = generics
                    .iter()
                    .filter_map(|param| match param {
                        GenericParam::Lifetime(lifetime) => Some(lifetime),
                        _ => None,
                    })
                    .any(|lt| &lt.lifetime == lifetime);
                if is_in_generics {
                    params.push(parse_quote! { #lifetime });
                }
            }
            params.extend(contains_generic_in_param(&reference.elem, generics));

            params
        }
        Type::Slice(slice) => contains_generic_in_param(&slice.elem, generics),
        Type::TraitObject(trait_obj) => trait_obj
            .bounds
            .iter()
            .flat_map(|bound| match bound {
                syn::TypeParamBound::Trait(trt) => {
                    if let Some(ident) = trt.path.get_ident() {
                        let is_in_generics = generics
                            .iter()
                            .filter_map(|param| match param {
                                GenericParam::Type(type_param) => Some(type_param),
                                _ => None,
                            })
                            .any(|type_param| &type_param.ident == ident);
                        if is_in_generics {
                            return vec![parse_quote! { #ident }];
                        }
                    }

                    vec![]
                }
                syn::TypeParamBound::Lifetime(lifetime) => {
                    let is_in_generics = generics
                        .iter()
                        .filter_map(|param| match param {
                            GenericParam::Lifetime(lifetime) => Some(lifetime),
                            _ => None,
                        })
                        .any(|lt| &lt.lifetime == lifetime);
                    if is_in_generics {
                        vec![parse_quote! { #lifetime }]
                    } else {
                        vec![]
                    }
                }
                _ => vec![],
            })
            .collect(),
        Type::Tuple(tuple) => tuple
            .elems
            .iter()
            .flat_map(|elem| contains_generic_in_param(elem, generics))
            .collect(),
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn expand(attr: TokenStream, item: TokenStream) -> String {
        let args = syn::parse2::<MessagesArgs>(attr).unwrap();
        Messages::parse(item, args)
            .unwrap()
            .into_token_stream()
            .to_string()
    }

    #[test]
    fn parses_messages_arg_without_outer_parentheses() {
        let args = syn::parse2::<MessagesArgs>(quote! { messages = ActorMessage }).unwrap();

        assert_eq!(args.messages.unwrap().to_string(), "ActorMessage");
        assert!(args.replies.is_none());
    }

    #[test]
    fn message_enum_uses_unit_and_tuple_variants() {
        let expanded = expand(
            quote! { messages = ActorMessage },
            quote! {
                impl Actor {
                    #[message]
                    fn reset(&self) {}

                    #[message(ctx)]
                    fn stop(&self, ctx: &mut Context<Self, ()>) {}

                    #[message]
                    fn inc(&mut self, amount: u32) {}
                }
            },
        );

        assert!(
            expanded.contains("enum ActorMessage { Reset , Stop , Inc (Inc) , }"),
            "{expanded}"
        );
    }

    #[test]
    fn message_enum_uses_common_handler_visibility() {
        let expanded = expand(
            quote! { messages = ActorMessage },
            quote! {
                impl Actor {
                    #[message]
                    pub fn reset(&self) {}

                    #[message]
                    pub fn inc(&mut self, amount: u32) {}
                }
            },
        );

        assert!(
            expanded.contains("pub enum ActorMessage { Reset , Inc (Inc) , }"),
            "{expanded}"
        );
    }

    #[test]
    fn message_enum_errors_on_mixed_handler_visibility() {
        let expanded = expand(
            quote! { messages = ActorMessage },
            quote! {
                impl Actor {
                    #[message]
                    pub fn reset(&self) {}

                    #[message]
                    fn inc(&mut self, amount: u32) {}
                }
            },
        );

        assert!(
            expanded.contains(
                "compile_error ! { \"message enum requires all message handlers to have the same visibility\" }"
            ),
            "{expanded}"
        );
    }

    #[test]
    fn message_enum_orders_impl_generics_before_function_generics() {
        let expanded = expand(
            quote! { messages = ActorMessage },
            quote! {
                impl<ActorValue> Actor<ActorValue> {
                    #[message]
                    pub fn first<T: Clone>(&self, actor: ActorValue, value: T) {}

                    #[message]
                    pub fn second<U: Copy>(&self, value: U) {}
                }
            },
        );

        assert!(
            expanded.contains(
                "pub enum ActorMessage < ActorValue , T : Clone , U : Copy > { First (First < ActorValue , T >) , Second (Second < U >) , }"
            ),
            "{expanded}"
        );
    }

    #[test]
    fn message_enum_renames_conflicting_function_generics() {
        let expanded = expand(
            quote! { messages = ActorMessage },
            quote! {
                impl Actor {
                    #[message]
                    pub fn first<T: Clone>(&self, value: T) {}

                    #[message]
                    pub fn second<T: Copy>(&self, value: T) {}
                }
            },
        );

        assert!(
            expanded.contains(
                "pub enum ActorMessage < T : Clone , T2 : Copy > { First (First < T >) , Second (Second < T2 >) , }"
            ),
            "{expanded}"
        );
    }

    #[test]
    fn message_enum_renames_conflicting_where_clause_generics() {
        let expanded = expand(
            quote! { messages = ActorMessage },
            quote! {
                impl Actor {
                    #[message]
                    pub fn first<T>(&self, value: T)
                    where
                        T: Clone,
                    {}

                    #[message]
                    pub fn second<T>(&self, value: T)
                    where
                        T: Copy,
                    {}
                }
            },
        );

        assert!(
            expanded.contains(
                "pub enum ActorMessage < T , T2 > where T : Clone , T2 : Copy { First (First < T >) , Second (Second < T2 >) , }"
            ),
            "{expanded}"
        );
    }

    #[test]
    fn response_enum_uses_unit_and_tuple_variants() {
        let expanded = expand(
            quote! { replies = ActorResponse },
            quote! {
                impl Actor {
                    #[message]
                    fn reset(&self) {}

                    #[message]
                    fn get_count(&self) -> i64 {}

                    #[message]
                    fn inc(&mut self, amount: u32) -> i64 {}
                }
            },
        );

        assert!(
            expanded.contains("enum ActorResponse { Reset , GetCount (i64) , Inc (i64) , }"),
            "{expanded}"
        );
    }

    #[test]
    fn response_enum_orders_impl_generics_before_function_generics() {
        let expanded = expand(
            quote! { replies = ActorResponse },
            quote! {
                impl<ActorValue> Actor<ActorValue> {
                    #[message]
                    pub fn first<T: Clone>(&self) -> (ActorValue, T) {}

                    #[message]
                    pub fn second<U: Copy>(&self) -> U {}
                }
            },
        );

        assert!(
            expanded.contains(
                "pub enum ActorResponse < ActorValue , T : Clone , U : Copy > { First ((ActorValue , T)) , Second (U) , }"
            ),
            "{expanded}"
        );
    }

    #[test]
    fn response_enum_renames_conflicting_function_generics() {
        let expanded = expand(
            quote! { replies = ActorResponse },
            quote! {
                impl Actor {
                    #[message]
                    pub fn first<T: Clone>(&self) -> T {}

                    #[message]
                    pub fn second<T: Copy>(&self) -> T {}
                }
            },
        );

        assert!(
            expanded.contains(
                "pub enum ActorResponse < T : Clone , T2 : Copy > { First (T) , Second (T2) , }"
            ),
            "{expanded}"
        );
    }

    #[test]
    fn dispatch_impl_generated_for_messages_and_replies() {
        let expanded = expand(
            quote! { messages = ActorMessage, replies = ActorResponse },
            quote! {
                impl Actor {
                    #[message]
                    pub fn reset(&self) {}

                    #[message]
                    pub fn inc(&mut self, amount: u32) -> i64 {}
                }
            },
        );

        // impl Dispatch<ActorMessage> for ActorRef<Actor>
        assert!(
            expanded.contains(
                "impl :: kameo :: message :: Dispatch < ActorMessage > for :: kameo :: actor :: ActorRef < Actor >"
            ),
            "{expanded}"
        );
        // type Response = ActorResponse
        assert!(
            expanded.contains("type Response = ActorResponse ;"),
            "{expanded}"
        );
        // unit variant: self.ask(Reset)
        assert!(
            expanded.contains("self . ask (Reset)"),
            "{expanded}"
        );
        // tuple variant: self.ask(msg)
        assert!(
            expanded.contains("self . ask (msg)"),
            "{expanded}"
        );
    }

    #[test]
    fn dispatch_impl_not_generated_without_replies() {
        let expanded = expand(
            quote! { messages = ActorMessage },
            quote! {
                impl Actor {
                    #[message]
                    pub fn inc(&mut self, amount: u32) -> i64 {}
                }
            },
        );

        assert!(
            !expanded.contains("Dispatch"),
            "dispatch impl should not be generated without replies = ...\n{expanded}"
        );
    }
}
