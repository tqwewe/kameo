mod derive_actor;
mod derive_remote_actor;
mod derive_reply;
mod messages;
mod remote_message;

use derive_actor::DeriveActor;
use derive_remote_actor::DeriveRemoteActor;
use derive_reply::DeriveReply;
use messages::Messages;
use proc_macro::TokenStream;
use quote::ToTokens;
use remote_message::{RemoteMessage, RemoteMessageAttrs};
use syn::parse_macro_input;

/// Attribute macro placed on `impl` blocks of actors to define messages.
///
/// Methods on the impl block are marked with `#[message]`.
/// This generates a struct for the message, allowing it to be sent to the actor.
///
/// # Example
///
/// ```ignore
/// use kameo::messages;
///
/// #[messages]
/// impl Counter {
///     /// Regular message
///     #[message]
///     pub fn inc(&mut self, amount: u32) -> i64 {
///         self.count += amount as i64;
///         self.count
///     }
///
///     /// Derives on the message
///     #[message(derive(Clone, Copy))]
///     pub fn dec(&self, amount: u32) {
///         self.count -= amount as i64;
///     }
/// }
///
/// counter_ref.ask(Inc { amount: 5 }).await?;
/// counter_ref.ask(Dec { amount: 2 }.clone()).await?;
/// ```
///
/// <details>
/// <summary>See expanded code</summary>
///
/// ```ignore
/// pub struct Inc {
///     pub amount: u32,
/// }
///
/// impl kameo::message::Message<Inc> for Counter {
///     type Reply = i64;
///
///     async fn handle(&mut self, msg: Counter, _ctx: &mut kameo::message::Context<Self, Self::Reply>) -> Self::Reply {
///         self.inc(msg.amount)
///     }
/// }
///
/// pub struct Count;
///
/// #[derive(Clone, Copy)]
/// pub struct Dec;
///
/// impl kameo::message::Message<Dec> for Counter {
///     type Reply = ();
///
///     async fn handle(&mut self, msg: Counter, _ctx: &mut kameo::message::Context<Self, Self::Reply>) -> Self::Reply {
///         self.dec(msg.amount)
///     }
/// }
/// ```
/// </details>
#[proc_macro_attribute]
pub fn messages(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let messages = parse_macro_input!(item as Messages);
    TokenStream::from(messages.into_token_stream())
}

/// Derive macro implementing the [Actor](https://docs.rs/kameo/latest/kameo/actor/trait.Actor.html) trait with default behaviour.
///
/// The `#[actor(name = "...")]` attribute can be specified to change the actors [Actor::name](https://docs.rs/kameo/latest/kameo/actor/trait.Actor.html#method.name).
/// The default value is the actor's ident.
///
/// The `#[actor(mailbox = ...)]` attribute can be specified to change the actors [Actor::Mailbox](https://docs.rs/kameo/latest/kameo/actor/trait.Actor.html#associatedtype.Mailbox).
/// The values can be one of:
///  - `bounded` (default capacity of 1000)
///  - `bounded(64)` (custom capacity of 64)
///  - `unbounded`
///
///
/// # Example
///
/// ```ignore
/// use kameo::Actor;
///
/// #[derive(Actor)]
/// #[actor(name = "my_amazing_actor", mailbox = bounded(256))]
/// struct MyActor { }
///
/// assert_eq!(MyActor::name(), "MyActor");
/// ```
#[proc_macro_derive(Actor, attributes(actor))]
pub fn derive_actor(input: TokenStream) -> TokenStream {
    let derive_actor = parse_macro_input!(input as DeriveActor);
    TokenStream::from(derive_actor.into_token_stream())
}

/// Derive macro implementing the [Reply](https://docs.rs/kameo/latest/kameo/reply/trait.Reply.html) trait as an infallible reply.
///
/// # Example
///
/// ```ignore
/// use kameo::Reply;
///
/// #[derive(Reply)]
/// struct Foo { }
/// ```
#[proc_macro_derive(Reply)]
pub fn derive_reply(input: TokenStream) -> TokenStream {
    let derive_reply = parse_macro_input!(input as DeriveReply);
    TokenStream::from(derive_reply.into_token_stream())
}

/// Derive macro implementing the [RemoteActor](https://docs.rs/kameo/latest/kameo/actor/remote/trait.RemoteActor.html)
/// trait with a default remote ID being the full path of the type being implemented.
///
/// The `#[remote_actor(id = "...")]` attribute can be specified to change the default remote actor ID.
///
/// # Example
///
/// ```ignore
/// use kameo::RemoteActor;
///
/// #[derive(RemoteActor)]
/// struct MyActor { }
///
/// assert_eq!(MyActor::REMOTE_ID, "my_crate::module::MyActor");
/// ```
#[proc_macro_derive(RemoteActor, attributes(remote_actor))]
pub fn derive_remote_actor(input: TokenStream) -> TokenStream {
    let derive_remote_actor = parse_macro_input!(input as DeriveRemoteActor);
    TokenStream::from(derive_remote_actor.into_token_stream())
}

/// Registers an actor message to be supported with remote messages.
///
/// # Example
///
/// ```ignore
/// use kameo::{remote_message, message::Message};
///
/// struct MyActor { }
/// struct MyMessage { }
///
/// #[remote_message("c6fa9f76-8818-4000-96f4-50c2ebd52408")]
/// impl Message<MyMessage> for MyActor {
///     // implementation here
/// }
/// ```
#[proc_macro_attribute]
pub fn remote_message(attrs: TokenStream, input: TokenStream) -> TokenStream {
    let remote_actor_attrs = parse_macro_input!(attrs as RemoteMessageAttrs);
    let remote_actor = parse_macro_input!(input as RemoteMessage);
    TokenStream::from(remote_actor.into_tokens(remote_actor_attrs))
}
