# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.13.0] - 2024-11-15

* @13r0ck made their first contribution in #79

### <!-- 0 -->Added

- Impl `IntoFuture` for requests (#72)
- Add public `BoxReplySender` type alias (#70)
- Add support for preparing an actor and running it outside a spawned task (#69)
- Add `Context::reply` shorthand method [</>](https://github.com/tqwewe/kameo/commit/5ebef48110d6d6e007f1d478251a4710203b33e5)

### <!-- 1 -->Changed

- **BREAKING:** Relax request impls to be generic to any mailbox (#71)
- **BREAKING:** Use owned actor ref in `spawn_with` function (#68)

### <!-- 3 -->Fixed

- **BREAKING:** Startup deadlock on small bounded mailboxes (#84)
- Tokio_unstable compile error in `spawn` [</>](https://github.com/tqwewe/kameo/commit/6b10a3b1b90d1e8dee29250851cfcd5d6bfc2934)
- Request downcasting and added tests (#85)

### <!-- 4 -->Documentation

- Remove reverences to deprecated linking methods (#79)

### <!-- 5 -->Misc

- Add empty message to git tag release script [</>](https://github.com/tqwewe/kameo/commit/c093fdf465c21fd98ff4aa27370dce730297e8e9)
- Remove msrv from Cargo.toml (#82)

## [0.12.2] - 2024-10-17

### <!-- 0 -->Added

- Add spawn_link and link/unlink functions (#67)

### <!-- 4 -->Documentation

- Update README with improved content [</>](https://github.com/tqwewe/kameo/commit/0265e48b5e51e73d1b98a9d9cf7738bf6f7c4eff)

### <!-- 5 -->Misc

- Add release script [</>](https://github.com/tqwewe/kameo/commit/74abeb18d7c5cfaa630990ad99cb07170e23af84)

## [0.12.1] - 2024-10-15

### <!-- 0 -->Added

- Add `ForwardMessageSendSync` request trait (#65)

### <!-- 3 -->Fixed

- Actor lifecycle error handling when `on_start` errors [</>](https://github.com/tqwewe/kameo/commit/5eb9249ff2f0ca8d450f6da33eb9f9021c93734d)

### <!-- 4 -->Documentation

- Add FAQ to book about reasons for actors stopping [</>](https://github.com/tqwewe/kameo/commit/5094e92ab0c4a89c05635c33380a4c2e1b41af8d)

### <!-- 5 -->Misc

- Ignore alpha and beta tags in cliff.toml [</>](https://github.com/tqwewe/kameo/commit/880f667a54789809566a68d5cab9f0f1f8955355)

## [0.12.0] - 2024-10-11

* @shusvr made their first contribution in #60
* @marcaddeo made their first contribution in #47

### <!-- 0 -->Added

- Add `ActorRef::wait_startup` method (#63)

### <!-- 1 -->Changed

- **BREAKING:** Make `Links` private (#57)
- **BREAKING:** Move actor pool and pubsub to their own modules (#56)
- **BREAKING:** Move `ActorIDFromBytesError` to `error` module [</>](https://github.com/tqwewe/kameo/commit/816eb931fe89a799b0a7cbda59b879d67a9ec329)
- **BREAKING:** Move remote actor functionality behind `remote` feature (#60) [</>](https://github.com/tqwewe/kameo/commit/ce4c73b7425ef1d6dce89585141fd8d13065b32e)
- ActorID and improve documentation (#48)

### <!-- 4 -->Documentation

- Remove benchmark from README.md [</>](https://github.com/tqwewe/kameo/commit/342b8e0247073e47f7696c93d020f9cf7b8009e2)
- Add contributors badge to README.md [</>](https://github.com/tqwewe/kameo/commit/2beefa5b916b660fcec425ce903de2016676de9a)
- Add Discord badge to README.md [</>](https://github.com/tqwewe/kameo/commit/ce6e74f89a3e669176dd6e197b6fc331052e97a7)
- Add book badge to README.md [</>](https://github.com/tqwewe/kameo/commit/b4f14c072919445bf7d5371a3cdf3b65da08bec9)
- Add getting help section to README.md [</>](https://github.com/tqwewe/kameo/commit/9e86c0e1a39a930d9c335aa9bab5897ee3a25795)
- Improve README with use cases, additional resources, and clearer structure [</>](https://github.com/tqwewe/kameo/commit/e2a2f94b20ef90d6fff1d019683ad99efa0ef98e)
- Add support section to README.md [</>](https://github.com/tqwewe/kameo/commit/8fc01b8fae217c3ec6bf5fe68cd450e829adb69c)
- Add Distributed Actor Communication section to README.md [</>](https://github.com/tqwewe/kameo/commit/66fd9473935f80e86498593b4eae5a1a338ef16d)
- Improve code docs for remote module [</>](https://github.com/tqwewe/kameo/commit/d8bb7172ca11909f9492ba79ee12367c9117f57b)
- Add in-depth distributed actors information to kameo book (#51)
- Update heading levels in book [</>](https://github.com/tqwewe/kameo/commit/7cbbc151299f3311b46fd439ff7c00bd9a039750)
- Improve code docs and examples with all tests passing (#54)
- Add FAQ to book (#59)

### <!-- 5 -->Misc

- Add links to README badges (#47)
- Add gtag to kameo book (#52)
- Add Github CI [</>](https://github.com/tqwewe/kameo/commit/68c3e125f4b09fc4063b724f653f69c5cf7baff7)
- Remote beta and nightly toolchains from CI [</>](https://github.com/tqwewe/kameo/commit/2ff1c5dde18753f25fa27046c5dfe63de080cde9)
- Create CODE_OF_CONDUCT.md [</>](https://github.com/tqwewe/kameo/commit/d0b2f5360ce0ac65d2334c01aecf6395617de373)
- Add CONTRIBUTING.md [</>](https://github.com/tqwewe/kameo/commit/c106d45ae8bbb8bc009be00c987d15eb4fbcc81b)
- Add github issue templates [</>](https://github.com/tqwewe/kameo/commit/463e3a707e1d56c240caa44084196d8e734809ec)
- Move banner.png into docs directory [</>](https://github.com/tqwewe/kameo/commit/ddc85ce8e36f1491f21258013ae2aa708966bcce)
- Add .envrc to .gitignore [</>](https://github.com/tqwewe/kameo/commit/79589c7fbd05fa63c962b35266c1ee199b2de8fc)
- Add pr detection to git cliff contributors [</>](https://github.com/tqwewe/kameo/commit/1f800bdc8be30e2d76b39a723a36bfabfa83813f)

## [0.11.0] - 2024-09-29


### <!-- 0 -->Added

- **BREAKING:** Add lifetime to requests to avoid mailbox cloning [</>](https://github.com/tqwewe/kameo/commit/4f363a3c24f623406bc8322a6f8fc0ef801beb12)
- Use interned peer ids for improved performance (#43)
- Return stream from join handle in `attach_stream` [</>](https://github.com/tqwewe/kameo/commit/c260c0f0493949f3eb4e3f6e6b3ec5419fe29ba3)

### <!-- 1 -->Changed

- **BREAKING:** Detach stream when actor stops [</>](https://github.com/tqwewe/kameo/commit/c778b9e2fb4c66dbd2ddac0c1eaff51b59732b3e)
- **BREAKING:** Use multiaddr and add `SwarmFuture` (#44) [</>](https://github.com/tqwewe/kameo/commit/c6275236c0bcfeb97cd52a7defcc45a1ab27f846)

### <!-- 3 -->Fixed

- `attach_stream` panicking when actor is stopped [</>](https://github.com/tqwewe/kameo/commit/711722898a52e216102dc9c7975eee26b6b28b25)

### <!-- 4 -->Documentation

- Add book explaining core concepts (#40)
- Add missing examples from actors page [</>](https://github.com/tqwewe/kameo/commit/71dcb6f46e69f233da7a5fbcf53c88a5471a7df4)
- Fix indentation for request features [</>](https://github.com/tqwewe/kameo/commit/0f9bf2d19ad6c642d7c90149f7512e5ff874d927)
- Add note about Result::Err in the reply trait [</>](https://github.com/tqwewe/kameo/commit/9dc17828c8cc6c556552858b0c1d6bf37517a821)
- Add note about `SendError::HandlerError` in replies [</>](https://github.com/tqwewe/kameo/commit/21e12beead500f85a8f4dc728103f10dda0dc874)
- Add icons and links to core concepts overview page [</>](https://github.com/tqwewe/kameo/commit/99cd4f9349502e1aebb49ffe635d806cc96e293c)
- Fix formatting in book [</>](https://github.com/tqwewe/kameo/commit/441fd0b53bc37e6f2d0174a71c3cb979d8234d12)
- Add icons to introduction headings [</>](https://github.com/tqwewe/kameo/commit/335d90280d7cddf93962d8977c380e28f2ef8828)

### <!-- 5 -->Misc

- Fix path to README in Cargo.toml [</>](https://github.com/tqwewe/kameo/commit/39c2cf4e88887d1954df353818c9b1e99b2b3e7d)
- Add obsidian related items to .gitignore [</>](https://github.com/tqwewe/kameo/commit/d08368632f68b1ada1de60e0f5bcc770b76cccaf)
- Remove unused mailbox modules [</>](https://github.com/tqwewe/kameo/commit/56c6844e083700fcea36b182e7e441d921f5eb86)
- Update git cliff configuration [</>](https://github.com/tqwewe/kameo/commit/d7e0bded948bb672d02d9757a9172ce0f8937477)

## [0.10.0] - 2024-09-09


### <!-- 0 -->Added

- **BREAKING:** Add request traits (#39)
- Add delayed_send for unbounded actors [</>](https://github.com/tqwewe/kameo/commit/cd2276b291b5e2ce736e867567c9ce1ad2506e31)
- Add remote actor support (#35)
- Add `actor` attribute to `Actor` derive macro [</>](https://github.com/tqwewe/kameo/commit/8a2543e5c5226bf89c1fbce715601dd3e2672400)
- Make actor swarm listen address optional [</>](https://github.com/tqwewe/kameo/commit/e23e1e49226263b68f061ad7cc74b119e242e98b)
- Use macro to clean request trait impls for `MaybeRequestTimeout` [</>](https://github.com/tqwewe/kameo/commit/bd3d00f9db718f4b9e087c5659ad05083ad95645)

### <!-- 2 -->Removed

- **BREAKING:** Remove queries (#36)

### <!-- 3 -->Fixed

- Call on_panic when actor panics during startup [</>](https://github.com/tqwewe/kameo/commit/8e46e3402cedaa9f1176cc45fb7a58f1d7340504)

### <!-- 4 -->Documentation

- Update README.md [</>](https://github.com/tqwewe/kameo/commit/28ca611fdc2d138eac7ae4051a15997f1c97c293)
- Improve documentation for async messages [</>](https://github.com/tqwewe/kameo/commit/bc019278f459d913cbac34da45d4b9f7fc899383)
- Add missing `mut` from `reply_sender` example [</>](https://github.com/tqwewe/kameo/commit/6f7b6f76b6043be426948da0a9621ed4e82018db)
- Add `MessageSend` import in code examples [</>](https://github.com/tqwewe/kameo/commit/5bad5553b3358eae26bea70f5acd5058f202993d)

### <!-- 5 -->Misc

- Fix path to README in Cargo.toml files [</>](https://github.com/tqwewe/kameo/commit/d7c8d7c487120c743b864a4d7629299858dfc53e)
- Move kameo crate to root directory [</>](https://github.com/tqwewe/kameo/commit/dadbf59164037f9f18a6912a1869220be8e500ad)
- Add banner image [</>](https://github.com/tqwewe/kameo/commit/7ccbfebed673d9d471e463285939761ce87995e8)
- Create dependabot.yml [</>](https://github.com/tqwewe/kameo/commit/fc16b842de61c05834c23bc63fdd88e72e387735)
- Remote PR number suffix from changelog generation [</>](https://github.com/tqwewe/kameo/commit/b9a13905b8f853c49700f4fbb872318acf4b03b4)

## [0.9.0] - 2024-06-25


### <!-- 0 -->Added

- **BREAKING:** Add support for bounded/unbounded mailboxes (#29)
- Add `Send + 'static` bounds to `Reply` trait [</>](https://github.com/tqwewe/kameo/commit/382a118966308697bfa4ca72dedacadc83107554)
- Add pubsub actor (#31) [</>](https://github.com/tqwewe/kameo/commit/27533843726f787c042425bacc2306a28e3f96b6)
- Add support for async pool factory functions (#33)
- Add async spawn_with function (#34)

### <!-- 1 -->Changed

- **BREAKING:** Return `SendError` from send methods allowing replies to be received blocking (#27)

### <!-- 3 -->Fixed

- Buffered messages not being applied correctly (#32)

### <!-- 5 -->Misc

- Update CHANGELOG.md [</>](https://github.com/tqwewe/kameo/commit/b059d59d4708d86ae00c5987fe682d8a36020b2f)
- Move crates out of nested `crates` dir [</>](https://github.com/tqwewe/kameo/commit/4d668657e26df2afde0a6acd44fe2f9f083e7453)

## [0.8.1] - 2024-05-24


### <!-- 0 -->Added

- Add `BlockingMessage` for blocking actor code (#26)

## [0.8.0] - 2024-04-19

* @liutaon made their first contribution in #21

### <!-- 0 -->Added

- Allow `ActorPool` itself to be spawned as an actor [</>](https://github.com/tqwewe/kameo/commit/deea594df98c620b562dd85af66efa123961ddf3)
- Add `SendError::flatten` method [</>](https://github.com/tqwewe/kameo/commit/08edb344a78f5606c5b63f1c1147fb90a6a4b9c5)
- Implement internal buffering whilst actor is starting up [</>](https://github.com/tqwewe/kameo/commit/c5b6fc228695caece1e260c51d9747a128c9e5f9)

### <!-- 1 -->Changed

- **BREAKING:** Use `StreamMessage` enum instead of trait [</>](https://github.com/tqwewe/kameo/commit/720002221618c85ef95e0b81a280ca34d2180737)
- **BREAKING:** Use `Display` implementation for handler errors [</>](https://github.com/tqwewe/kameo/commit/da888c08c72a5c506fb4b716d62f3011b34c1e2c)

### <!-- 2 -->Removed

- Remove `Sync` requirement from `Reply` macro

### <!-- 3 -->Fixed

- `is_alive` returning the opposite value [</>](https://github.com/tqwewe/kameo/commit/bb33aeab5ee76f9711c0fb2cac78e0b01d4cff80)

### <!-- 4 -->Documentation

- Add example to `Reply` trait code docs [</>](https://github.com/tqwewe/kameo/commit/9c52c46ab559a49fe4ba18deb2dbfcc74f1ad678)

### <!-- 5 -->Misc

- Add CHANGELOG.md [</>](https://github.com/tqwewe/kameo/commit/a3ab7e589b5873cabf12583f3ca5b6b7d70c5538)
- Update cliff.toml [</>](https://github.com/tqwewe/kameo/commit/ec2c66c21db16e1546592d2228e70481ddb57cd8)
- Add newline for new contributors in cliff config [</>](https://github.com/tqwewe/kameo/commit/84f5f1ba253fe188a9a419255da871e111b024a4)

## [0.7.0] - 2024-04-15


### <!-- 0 -->Added

- **BREAKING:** Add values to `StreamMessage::on_start` and `StreamMessage::on_finish` [</>](https://github.com/tqwewe/kameo/commit/3427b012baacf88bbe2341606eaef0be93929a48)
- Add support for actor generics in `messages` macro [</>](https://github.com/tqwewe/kameo/commit/e1eee7607f9c0ed63cf9f78d06b808a74e5ca8a1)
- Add stream messages to forward messages from a stream to an actor [</>](https://github.com/tqwewe/kameo/commit/22aad1d7ca58b946b439f80d1e394b3079a21066)

### <!-- 2 -->Removed

- **BREAKING:** Remove stateless actors [</>](https://github.com/tqwewe/kameo/commit/1836857beb1bdf07e037089afe3cfb3f2443de74)

### <!-- 5 -->Misc

- Remove unused dependency `trait-variant` [</>](https://github.com/tqwewe/kameo/commit/7f3c3a7aae9b11b90f5bd42dc532c7f9221d5436)
- Add overhead benchmark [</>](https://github.com/tqwewe/kameo/commit/4aacfb7144cdded36e25c7a5d0f5f303c69c9ff4)
- Remove commented stateless actor code [</>](https://github.com/tqwewe/kameo/commit/cb350f0d743ba6d8ab82cca30ef57d9e24fc8467)
- Add git cliff integration [</>](https://github.com/tqwewe/kameo/commit/ff5b29b1b7bb984ded2f6555e7b53c2244f8688f)

## [0.6.0] - 2024-04-11


### <!-- 0 -->Added

- **BREAKING:** Add delegated reply with context type [</>](https://github.com/tqwewe/kameo/commit/56fa73c2ddd5face97f39e910d814d4bf4a318b3)

### <!-- 1 -->Changed

- **BREAKING:** Move all types to separate modules and improve documentation [</>](https://github.com/tqwewe/kameo/commit/62bc218822f288f22c19f902e8562032fea7510e)

### <!-- 2 -->Removed

- **BREAKING:** Remove `Spawn` trait and use spawn functions [</>](https://github.com/tqwewe/kameo/commit/0d24cc52fe3f91c20e61a9853fbdc98acc09def5)

### <!-- 4 -->Documentation

- Improve docs for spawn functions [</>](https://github.com/tqwewe/kameo/commit/a3104deb560e4063d03e48719b4a4f5cbc9f3e2a)
- Add note to `Actor` derive macro [</>](https://github.com/tqwewe/kameo/commit/98407ed701bf3a8bacf954acc5a93b941efcbe33)
- Add missing `Context` param from docs [</>](https://github.com/tqwewe/kameo/commit/e54de6c404ffa8ebabf4ad9cf0593871eeb1ade4)

## [0.5.0] - 2024-04-04


### <!-- 0 -->Added

- Add `HandlerError` to `SendError` to flatten actor errors [</>](https://github.com/tqwewe/kameo/commit/842957880e4c3183054486e1b1b560626477bcda)

### <!-- 2 -->Removed

- **BREAKING:** Remove `nightly` flag and implement `Reply` on common types and derive macro [</>](https://github.com/tqwewe/kameo/commit/d4015d6960fd80fe193163e6f76cb349832918fe)

### <!-- 4 -->Documentation

- Remove spawn from `ActorPool` example [</>](https://github.com/tqwewe/kameo/commit/55c0defc73ce18612cc6d8c2b9c19a2588201997)
- Improve docs for QueriesNotSupported error [</>](https://github.com/tqwewe/kameo/commit/bc8ff6a5e6b09bd114bcd218b90fd66b34905b4a)

## [0.4.0] - 2024-04-03


### <!-- 0 -->Added

- Add debug assert to protect against deadlocks [</>](https://github.com/tqwewe/kameo/commit/8765bbd0eebbc1b73b6f041cd035e5eda339a72a)
- Add `ActorPool` [</>](https://github.com/tqwewe/kameo/commit/56636a9d8de372f46e75ee48d8c30c4321cafb7b)

### <!-- 1 -->Changed

- **BREAKING:** Impl `Message` and `Query` for actor instead of message type [</>](https://github.com/tqwewe/kameo/commit/69698230d738864ce2209203e70b5da8df65cd0d)

### <!-- 4 -->Documentation

- Add shields to readme [</>](https://github.com/tqwewe/kameo/commit/9736057d822429a79dc9483cfb9be28da2b7c64e)

### <!-- 5 -->Misc

- Delete empty `mailbox.rs` module [</>](https://github.com/tqwewe/kameo/commit/bb4c8ffdbda8bd4f56cde00ffe8bc38e7d557197)
- Rename bench fibonachi to fibonacci [</>](https://github.com/tqwewe/kameo/commit/7d1d81ae3a98590d6bb6b1ccb7fec2663cbacf47)

## [0.3.4] - 2024-04-02


### <!-- 3 -->Fixed

- Parsing of message attributes [</>](https://github.com/tqwewe/kameo/commit/1cabf3d7df97800673df38796235befd7f89cf26)

## [0.3.3] - 2024-04-01


### <!-- 0 -->Added

- Add local non-send/sync support [</>](https://github.com/tqwewe/kameo/commit/c872631b97ac17f4944048726bc16fbaf1bd775e)
- Add support for `!Sync` actors [</>](https://github.com/tqwewe/kameo/commit/cbf3ce185296ade3c73bbfdfbab88bbf4a3618e8)
- Add benchmarks [</>](https://github.com/tqwewe/kameo/commit/aa31989fd7c3efd27ec1a5e5728f1cc6b2433eda)
- Remove _unsync methods for nightly [</>](https://github.com/tqwewe/kameo/commit/8646dc51dadbf6dd7da1bf5df37e2d7f52e13b34)

### <!-- 4 -->Documentation

- Improve readme and crate docs [</>](https://github.com/tqwewe/kameo/commit/3d502392f281dbc9ba532d7e4fb0feb614b458ac)

### <!-- 5 -->Misc

- Fix bench name [</>](https://github.com/tqwewe/kameo/commit/5496515f678d889c07c9e2c3ac5e96c5665a46ca)
- Update crate descriptions [</>](https://github.com/tqwewe/kameo/commit/d79fd0a40d36c553140ee7a7597ed47c20495237)

## [0.3.2] - 2024-03-31


### <!-- 3 -->Fixed

- Only validate methods marked as message or query [</>](https://github.com/tqwewe/kameo/commit/5d92d80785cd9b76a322d83bfa018f38393af0b4)

### <!-- 4 -->Documentation

- Pimp README [</>](https://github.com/tqwewe/kameo/commit/7e3fdbaa968b5b2067512ac0986dae67b48be145)

### <!-- 5 -->Misc

- Add license files [</>](https://github.com/tqwewe/kameo/commit/4f960ac598109d3775c7d4b4e6f80e39354a988b)
- Create FUNDING.yml [</>](https://github.com/tqwewe/kameo/commit/996e9ef58bc6da7ca713ad8d65d1b887b95ec1c2)
- Add `.DS_Store` to .gitignore [</>](https://github.com/tqwewe/kameo/commit/7fbb76cdd0a6c3f48f0793f08979448e4ef7a121)

## [0.3.1] - 2024-03-30


### <!-- 4 -->Documentation

- Update install version [</>](https://github.com/tqwewe/kameo/commit/f5a543ee69d527f0343f83aa881f2399f1b4e2a8)

## [0.3.0] - 2024-03-30


### <!-- 0 -->Added

- Add macros [</>](https://github.com/tqwewe/kameo/commit/c3d81cf9a566e527c13b6f21b4bcde63bba5c93d)

## [0.2.0] - 2024-03-30


### <!-- 0 -->Added

- Remove async_trait from public traits [</>](https://github.com/tqwewe/kameo/commit/569ad4418655a253b5ffbfb97d08e2240c1270c8)

## [0.1.2] - 2024-03-29


### <!-- 0 -->Added

- Re-export async_trait [</>](https://github.com/tqwewe/kameo/commit/066ad2cbf1a16b2666dcaa1afe620123c64f2f13)

### <!-- 4 -->Documentation

- Fix nightly panic info [</>](https://github.com/tqwewe/kameo/commit/8575878854fa098343e5f02cea7b67511140b676)
- Add installing section to docs [</>](https://github.com/tqwewe/kameo/commit/8e2d1966ba7510458f7bfeff0976a0fb50b4e7cd)

## [0.1.1] - 2024-03-29


### <!-- 0 -->Added

- Add support for stable rust [</>](https://github.com/tqwewe/kameo/commit/0d3e66c47ab04d435bf44c356b1e0ff53f78e43e)

[0.12.1]: https://github.com/tqwewe/kameo/compare/v0.12.0..0.12.1
[0.12.0]: https://github.com/tqwewe/kameo/compare/v0.11.0..0.12.0
[0.11.0]: https://github.com/tqwewe/kameo/compare/v0.10.0..v0.11.0
[0.10.0]: https://github.com/tqwewe/kameo/compare/v0.9.0..v0.10.0
[0.9.0]: https://github.com/tqwewe/kameo/compare/v0.8.1..v0.9.0
[0.8.1]: https://github.com/tqwewe/kameo/compare/v0.8.0..v0.8.1
[0.8.0]: https://github.com/tqwewe/kameo/compare/v0.7.0..v0.8.0
[0.7.0]: https://github.com/tqwewe/kameo/compare/v0.6.0..v0.7.0
[0.6.0]: https://github.com/tqwewe/kameo/compare/v0.5.0..v0.6.0
[0.5.0]: https://github.com/tqwewe/kameo/compare/v0.4.0..v0.5.0
[0.4.0]: https://github.com/tqwewe/kameo/compare/v0.3.4..v0.4.0
[0.3.4]: https://github.com/tqwewe/kameo/compare/v0.3.3..v0.3.4
[0.3.3]: https://github.com/tqwewe/kameo/compare/v0.3.2..v0.3.3
[0.3.2]: https://github.com/tqwewe/kameo/compare/v0.3.1..v0.3.2
[0.3.1]: https://github.com/tqwewe/kameo/compare/v0.3.0..v0.3.1
[0.3.0]: https://github.com/tqwewe/kameo/compare/v0.2.0..v0.3.0
[0.2.0]: https://github.com/tqwewe/kameo/compare/v0.1.2..v0.2.0
[0.1.2]: https://github.com/tqwewe/kameo/compare/v0.1.1..v0.1.2
[0.1.1]: https://github.com/tqwewe/kameo/compare/v0.1.0..v0.1.1

<!-- generated by git-cliff -->
