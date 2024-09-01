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
/// Methods on the impl block are marked with `#[message]` or `#[query]`.
/// This generates a struct for the message, allowing it to be sent to the actor.
///
/// # Example
///
/// ```
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
///     /// Regular query
///     #[query]
///     pub fn count(&self) -> i64 {
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
/// counter_ref.ask(Inc { amount: 5 }).send().await?;
/// counter_ref.query(Count).send().await?;
/// counter_ref.ask(Dec { amount: 2 }.clone()).send().await?;
/// ```
///
/// <details>
/// <summary>See expanded code</summary>
///
/// ```
/// pub struct Inc {
///     pub amount: u32,
/// }
///
/// impl kameo::message::Message<Inc> for Counter {
///     type Reply = i64;
///
///     async fn handle(&mut self, msg: Counter, _ctx: kameo::message::Context<'_, Self, Self::Reply>) -> Self::Reply {
///         self.inc(msg.amount)
///     }
/// }
///
/// pub struct Count;
///
/// impl kameo::message::Query<Count> for Counter {
///     type Reply = i64;
///
///     async fn handle(&self, msg: Counter) -> Self::Reply {
///         self.count()
///     }
/// }
///
/// #[derive(Clone, Copy)]
/// pub struct Dec;
///
/// impl kameo::message::Message<Dec> for Counter {
///     type Reply = ();
///
///     async fn handle(&mut self, msg: Counter, _ctx: kameo::message::Context<'_, Self, Self::Reply>) -> Self::Reply {
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
/// The [Actor::name](https://docs.rs/kameo/latest/kameo/actor/trait.Actor.html#method.name) is implemented using the actor's ident.
///
/// This trait has no customizability, and is only a convenience macro for implementing `Actor`.
/// If you'd like to override the default behaviour, you should implement the `Actor` trait manually.
///
/// # Example
///
/// ```
/// use kameo::Actor;
///
/// #[derive(Actor)]
/// struct MyActor { }
///
/// assert_eq!(MyActor { }.name(), "MyActor");
/// ```
#[proc_macro_derive(Actor)]
pub fn derive_actor(input: TokenStream) -> TokenStream {
    let derive_actor = parse_macro_input!(input as DeriveActor);
    TokenStream::from(derive_actor.into_token_stream())
}

/// Derive macro implementing the [Reply](https://docs.rs/kameo/latest/kameo/reply/trait.Reply.html) trait as an infallible reply.
///
/// # Example
///
/// ```
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
/// ```
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
/// ```
/// use kameo::{remote_message, message::Message};
///
/// struct MyActor { }
/// struct MyMessage { }
///
/// #[remote_message("c6fa9f76-8818-4000-96f4-50c2ebd52408")]
/// impl Message<MyMessage> for MyActor { ... }
/// ```
#[proc_macro_attribute]
pub fn remote_message(attrs: TokenStream, input: TokenStream) -> TokenStream {
    let remote_actor_attrs = parse_macro_input!(attrs as RemoteMessageAttrs);
    let remote_actor = parse_macro_input!(input as RemoteMessage);
    TokenStream::from(remote_actor.into_tokens(remote_actor_attrs))
}
