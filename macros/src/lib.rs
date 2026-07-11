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
/// Methods on the impl block are marked with `#[message]`.
/// This generates a struct for the message, allowing it to be sent to the actor.
///
/// # Attributes
///
/// - `#[message]` - Basic message definition
/// - `#[message(derive(...))]` - Add derives to the generated message struct
/// - `#[message(ctx)]` - Include a `ctx` parameter that is excluded from the generated struct
/// - `#[message(ctx = name)]` - Include a context parameter with a custom name
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
///
///     /// Message with context parameter
///     #[message(ctx)]
///     pub async fn inc_with_logging(&mut self, amount: u32, ctx: &mut Context<Self, i64>) -> i64 {
///         // ctx is available but not part of the message struct
///         self.count += amount as i64;
///         self.count
///     }
///
///     /// Message with custom context parameter name
///     #[message(ctx = my_ctx)]
///     pub async fn reset(&mut self, my_ctx: &mut Context<Self, ()>) {
///         self.count = 0;
///     }
/// }
///
/// counter_ref.ask(Inc { amount: 5 }).await?;
/// counter_ref.ask(Dec { amount: 2 }.clone()).await?;
/// counter_ref.ask(IncWithLogging { amount: 3 }).await?;
/// counter_ref.ask(Reset).await?;
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
///     async fn handle(&mut self, msg: Inc, _ctx: &mut kameo::message::Context<Self, Self::Reply>) -> Self::Reply {
///         self.inc(msg.amount)
///     }
/// }
///
/// #[derive(Clone, Copy)]
/// pub struct Dec {
///     pub amount: u32,
/// }
///
/// impl kameo::message::Message<Dec> for Counter {
///     type Reply = ();
///
///     async fn handle(&mut self, msg: Dec, _ctx: &mut kameo::message::Context<Self, Self::Reply>) -> Self::Reply {
///         self.dec(msg.amount)
///     }
/// }
///
/// pub struct IncWithLogging {
///     pub amount: u32,
///     // Note: ctx is NOT included in the struct
/// }
///
/// impl kameo::message::Message<IncWithLogging> for Counter {
///     type Reply = i64;
///
///     async fn handle(&mut self, msg: IncWithLogging, ctx: &mut kameo::message::Context<Self, Self::Reply>) -> Self::Reply {
///         self.inc_with_logging(msg.amount, ctx).await
///     }
/// }
///
/// pub struct Reset;
///
/// impl kameo::message::Message<Reset> for Counter {
///     type Reply = ();
///
///     async fn handle(&mut self, msg: Reset, my_ctx: &mut kameo::message::Context<Self, Self::Reply>) -> Self::Reply {
///         self.reset(my_ctx).await
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
