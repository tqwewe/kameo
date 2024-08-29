use quote::{quote, ToTokens};
use syn::{
    parse::{Parse, ParseStream},
    DeriveInput, Ident,
};

pub struct AttrRemoteMessage {
    actor_ident: Ident,
}

impl ToTokens for AttrRemoteMessage {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let Self { ident } = self;

        tokens.extend(quote! {
            const _: () = {
                #[::kameo::actor::remote::_internal::distributed_slice(
                    ::kameo::actor::remote::_internal::REMOTE_MESSAGES
                )]
                static REG: (
                    ::kameo::actor::remote::_internal::RemoteMessageRegistrationID<'static>,
                    (
                        ::kameo::actor::remote::_internal::AskRemoteMessageFn,
                        ::kameo::actor::remote::_internal::TellRemoteMessageFn,
                    ),
                ) = (
                    ::kameo::actor::remote::_internal::RemoteMessageRegistrationID {
                        actor_name: <$actor_ty as ::kameo::actor::remote::RemoteActor>::REMOTE_ID,
                        message_name: <$message_ty as ::kameo::actor::remote::RemoteMessage>::REMOTE_ID,
                    },
                    (
                        (|actor_id: ::kameo::actor::ActorID,
                          msg: ::std::vec::Vec<u8>,
                          mailbox_timeout: Option<Duration>,
                          reply_timeout: Option<Duration>,
                          immediate: bool| {
                            ::std::boxed::Box::pin(
                                ::kameo::actor::remote::_internal::ask_remote_message::<
                                    $actor_ty,
                                    $message_ty,
                                >(
                                    actor_id, msg, mailbox_timeout, reply_timeout, immediate
                                ),
                            )
                        }) as ::kameo::actor::remote::_internal::AskRemoteMessageFn,
                        (|actor_id: ::kameo::actor::ActorID,
                          msg: ::std::vec::Vec<u8>,
                          mailbox_timeout: Option<Duration>,
                          immediate: bool| {
                            ::std::boxed::Box::pin(
                                ::kameo::actor::remote::_internal::tell_remote_message::<
                                    $actor_ty,
                                    $message_ty,
                                >(actor_id, msg, mailbox_timeout, immediate),
                            )
                        }) as ::kameo::actor::remote::_internal::TellRemoteMessageFn,
                    ),
                );
            };
        });
    }
}

impl Parse for AttrRemoteMessage {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let input: DeriveInput = input.parse()?;
        let ident = input.ident;

        Ok(AttrRemoteMessage { ident })
    }
}
