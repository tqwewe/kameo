mod derive_actor;
mod derive_reply;
mod derive_remote_message;
mod messages;

use derive_actor::DeriveActor;
use derive_reply::DeriveReply;
use derive_remote_message::DeriveRemoteMessage;
use messages::Messages;
use proc_macro::TokenStream;
use quote::ToTokens;
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

/// Derive macro implementing type hashes for remote messages.
///
/// This generates the `HasTypeHash` implementation needed for remote messaging
/// without requiring the full `distributed_actor!` macro on the client side.
///
/// # Example
///
/// ```ignore
/// use kameo::RemoteMessage;
/// use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};
///
/// #[derive(RemoteMessage, Archive, RSerialize, RDeserialize)]
/// struct LogMessage {
///     level: String,
///     content: String,
/// }
/// ```
#[proc_macro_derive(RemoteMessage)]
pub fn derive_remote_message(input: TokenStream) -> TokenStream {
    let derive_remote_message = parse_macro_input!(input as DeriveRemoteMessage);
    TokenStream::from(derive_remote_message.into_token_stream())
}



/// Attribute macro that automatically adds the required derives for remote messages.
///
/// This macro automatically applies:
/// - `RemoteMessage` derive for type hashing and remote messaging support  
/// - `rkyv::Archive`, `rkyv::Serialize`, `rkyv::Deserialize` for binary serialization
///
/// # Example
///
/// ```ignore
/// use kameo::remote_message_derive;
///
/// #[remote_message_derive]
/// struct LogMessage {
///     level: String,
///     content: String,
/// }
/// ```
///
/// This is equivalent to:
/// ```ignore
/// #[derive(RemoteMessage, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
/// struct LogMessage {
///     level: String,
///     content: String,
/// }
/// ```
#[proc_macro_attribute]
pub fn remote_message_derive(_attrs: TokenStream, input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as syn::DeriveInput);
    
    // Add the required derive attributes
    let derives = vec![
        "::kameo_macros::RemoteMessage",
        "::rkyv::Archive",
        "::rkyv::Serialize",
        "::rkyv::Deserialize",
    ];
    
    // Parse the derive paths
    let derive_paths: Vec<syn::Path> = derives
        .into_iter()
        .map(|d| syn::parse_str(d).unwrap())
        .collect();
    
    // Create the derive attribute
    let derive_attr = syn::Attribute {
        pound_token: syn::Token![#](proc_macro2::Span::call_site()),
        style: syn::AttrStyle::Outer,
        bracket_token: syn::token::Bracket(proc_macro2::Span::call_site()),
        meta: syn::Meta::List(syn::MetaList {
            path: syn::parse_str("derive").unwrap(),
            delimiter: syn::MacroDelimiter::Paren(syn::token::Paren(proc_macro2::Span::call_site())),
            tokens: {
                let mut tokens = proc_macro2::TokenStream::new();
                for (i, path) in derive_paths.iter().enumerate() {
                    if i > 0 {
                        tokens.extend(quote::quote! { , });
                    }
                    tokens.extend(quote::quote! { #path });
                }
                tokens
            },
        }),
    };
    
    // Add the derive attribute to the struct
    input.attrs.push(derive_attr);
    
    TokenStream::from(quote::quote! { #input })
}
