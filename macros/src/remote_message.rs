use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    spanned::Spanned,
    AngleBracketedGenericArguments, GenericArgument, Generics, ItemImpl, LitStr, PathArguments,
    PathSegment, Token, Type,
};
use uuid::Uuid;

pub struct RemoteMessageAttrs {
    id: LitStr,
}

impl Parse for RemoteMessageAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            let random_uuid = Uuid::new_v4();
            return Err(syn::Error::new(
                input.span(),
                format!("expected remote message id\nhere's a random uuid you can use:\n  #[remote_message(\"{random_uuid}\")]"),
            ));
        }
        Ok(RemoteMessageAttrs { id: input.parse()? })
    }
}

pub struct RemoteMessage {
    item_impl: ItemImpl,
    actor_ty: Box<Type>,
    actor_generics: Generics,
    message_generics: Punctuated<GenericArgument, Token![,]>,
}

impl RemoteMessage {
    pub fn into_tokens(self, attrs: RemoteMessageAttrs) -> TokenStream {
        let RemoteMessageAttrs { id } = attrs;
        let Self {
            item_impl,
            actor_ty,
            actor_generics,
            message_generics,
        } = self;

        let (impl_generics, ty_generics, where_clause) = actor_generics.split_for_impl();

        quote! {
            #item_impl

            #[automatically_derived]
            impl #impl_generics ::kameo::remote::RemoteMessage<#message_generics> for #actor_ty #ty_generics #where_clause {
                const REMOTE_ID: &'static str = #id;
            }

            // Remove linkme registration entirely
            // The new distributed_actor! macro will handle registration via type hashes
        }
    }
}

impl Parse for RemoteMessage {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let item_impl: ItemImpl = input.parse()?;
        let input_span = item_impl.span();
        let actor_ty = item_impl.self_ty.clone();
        let actor_generics = item_impl.generics.clone();
        let (_, trait_path, _) = item_impl.trait_.as_ref().ok_or_else(|| {
            syn::Error::new(
                input_span,
                "remote message can only be used on an impl for kameo::message::Message",
            )
        })?;
        let trait_path_span = trait_path.span();
        let PathSegment {
            ident: _,
            arguments,
        } = trait_path
            .segments
            .last()
            .ok_or_else(|| syn::Error::new(trait_path_span, "expected trait path"))?
            .clone();
        let PathArguments::AngleBracketed(AngleBracketedGenericArguments {
            args: message_generics,
            ..
        }) = arguments
        else {
            return Err(syn::Error::new(
                trait_path_span,
                "expected angle bracket arguments",
            ));
        };

        Ok(RemoteMessage {
            item_impl,
            actor_ty,
            actor_generics,
            message_generics,
        })
    }
}
