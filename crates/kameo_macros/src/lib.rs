mod derive_actor;
mod derive_reply;
mod messages;

use derive_actor::DeriveActor;
use derive_reply::DeriveReply;
use messages::Messages;
use proc_macro::TokenStream;
use quote::ToTokens;
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
/// counter_ref.send(Inc { amount: 5 }).await?;
/// counter_ref.query(Count).await?;
/// counter_ref.send(Dec { amount: 2 }.clone()).await?;
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
///     async fn handle(&mut self, msg: Counter) -> Self::Reply {
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
///     async fn handle(&mut self, msg: Counter) -> Self::Reply {
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
