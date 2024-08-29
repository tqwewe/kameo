use quote::{quote, ToTokens};
use syn::{
    parse::{Parse, ParseStream},
    DeriveInput, Ident,
};

pub struct DeriveRemoteActor {
    ident: Ident,
}

impl ToTokens for DeriveRemoteActor {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let Self { ident } = self;

        tokens.extend(quote! {
            #[automatically_derived]
            impl ::kameo::remote::RemoteActor for #ident {
                const REMOTE_ID: &'static str =
                    ::std::concat!(::std::module_path!(), "::", ::std::stringify!(#ident));
            }

            const _: () = {
                #[::kameo::remote::_internal::distributed_slice(
                    ::kameo::remote::_internal::REMOTE_ACTORS
                )]
                static REG: (
                    &'static str,
                    ::kameo::remote::_internal::RemoteSpawnFn,
                ) = (
                    <#ident as ::kameo::remote::RemoteActor>::REMOTE_ID,
                    (|actor: ::std::vec::Vec<u8>| {
                        ::std::boxed::Box::pin(::kameo::remote::_internal::spawn_remote::<
                            #ident,
                        >(actor))
                    }) as ::kameo::remote::_internal::RemoteSpawnFn,
                );
            };
        });
    }
}

impl Parse for DeriveRemoteActor {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let input: DeriveInput = input.parse()?;
        let ident = input.ident;

        Ok(DeriveRemoteActor { ident })
    }
}
