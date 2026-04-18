mod derive_actor;
mod derive_remote_actor;
mod derive_reply;
mod messages;
mod remote_message;

use derive_actor::DeriveActor;
use derive_remote_actor::DeriveRemoteActor;
use derive_reply::DeriveReply;
use messages::{Messages, MessagesArgs};
use proc_macro::TokenStream;
use quote::ToTokens;
use remote_message::{RemoteMessage, RemoteMessageAttrs};
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
/// - `#[messages(enum = Name)]` - Generate an enum with one variant per generated message
///   struct, plus a reply enum named `NameReply`. Messages with fields become tuple variants,
///   and messages without fields become unit variants.
///   The macro also implements [`Message<MessageEnum>`](kameo::message::Message) for the actor so
///   that `actor_ref.ask(msg_enum_variant).send().await` returns
///   `Result<ReplyEnum, SendError<MessageEnum>>`.
///
/// # Example
///
/// ```ignore
/// use kameo::messages;
///
/// #[messages(enum = CounterMessage)]
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
/// ## Aggregated message enum with unified reply enum
///
/// When `messages` is provided, you can dispatch any variant of the generated message enum
/// through a single `ask` call and receive a typed reply enum back.
///
/// ```ignore
/// use kameo::prelude::*;
///
/// #[messages(enum = CounterMessage)]
/// impl Counter {
///     #[message]
///     pub fn inc(&mut self, amount: u32) -> i64 { /* … */ }
///
///     #[message]
///     pub fn dec(&mut self, amount: u32) -> i64 { /* … */ }
///
///     #[message]
///     pub fn reset(&mut self) {}
///
///     #[message]
///     pub fn get(&self) -> i64 { /* … */ }
/// }
///
/// // Any variant of `CounterMessage` can be sent with a single `ask` call.
/// // The return type is `Result<CounterMessageReply, SendError<CounterMessage>>`.
/// let response: CounterMessageReply = actor_ref.ask(CounterMessage::Inc(Inc { amount: 5 })).send().await?;
///
/// match response {
///     CounterMessageReply::Inc(v) | CounterMessageReply::Dec(v) | CounterMessageReply::Get(v) => {
///         println!("value: {v}");
///     }
///     CounterMessageReply::Reset => {}
/// }
/// ```
///
/// <details>
/// <summary>See expanded code</summary>
///
/// ```ignore
/// // --- individual message structs and their Message impls ---
///
/// pub struct Inc { pub amount: u32 }
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
/// pub struct Dec { pub amount: u32 }
///
/// impl kameo::message::Message<Dec> for Counter {
///     type Reply = ();
///
///     async fn handle(&mut self, msg: Dec, _ctx: &mut kameo::message::Context<Self, Self::Reply>) -> Self::Reply {
///         self.dec(msg.amount)
///     }
/// }
///
/// pub struct IncWithLogging { pub amount: u32 }  // ctx excluded
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
///
/// // --- aggregated message enum (messages = CounterMessage) ---
///
/// pub enum CounterMessage {
///     Inc(Inc),
///     Dec(Dec),
///     IncWithLogging(IncWithLogging),
///     Reset,
/// }
///
/// // --- aggregated reply enum (CounterMessageReply) ---
/// // Generated automatically from the message enum name.
///
/// pub enum CounterMessageReply {
///     Inc(i64),
///     Dec(i64),       // Dec returns () so the variant is unit here — omitted for clarity
///     IncWithLogging(i64),
///     Reset,
/// }
///
/// impl kameo::Reply for CounterMessageReply {
///     type Ok = Self;
///     type Error = kameo::error::Infallible;
///     type Value = Self;
///     // … infallible impl …
/// }
///
/// // --- Message<CounterMessage> dispatch impl ---
/// // Routes each enum variant to the corresponding inner handler.
/// // `actor_ref.ask(CounterMessage::…).send().await` returns
/// // `Result<CounterMessageReply, SendError<CounterMessage>>`.
///
/// impl kameo::message::Message<CounterMessage> for Counter {
///     type Reply = CounterResponse;
///
///     async fn handle(
///         &mut self,
///         msg: CounterMessage,
///         ctx: &mut kameo::message::Context<Self, Self::Reply>,
///     ) -> Self::Reply {
///         match msg {
///             CounterMessage::Inc(inner) => {
///                 let reply = /* dispatch to Message<Inc>::handle */ …;
///                 CounterResponse::Inc(reply)
///             }
///             CounterMessage::Reset => {
///                 /* dispatch to Message<Reset>::handle */;
///                 CounterResponse::Reset
///             }
///             // … other variants …
///         }
///     }
/// }
/// ```
/// </details>
#[proc_macro_attribute]
pub fn messages(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as MessagesArgs);

    match Messages::parse(item.into(), args) {
        Ok(m) => TokenStream::from(m.into_token_stream()),
        Err(err) => err.to_compile_error().into(),
    }
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
/// #[remote_message]
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
