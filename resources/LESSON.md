## Rustに関する学び

- Nodeがワークロードによらないnode_idやnode_idsの管理とメッセージのやり取りを担当する構造体
- NodeにHandlerトレイトを実装した構造体をadd_handlerすることで特定のワークロードに対応させる
  - Node::handleでハンドラを呼び出す形式にしようとすると、Node::handle(self)ですでに可変参照をとっているのにハンドラの引数にも可変参照を渡さないといけなくなって借用チェッカーに指摘される
  - これに対応するため、NodeはCloneができるように内部のフィールドをRcで格納するべきだと思われる
  - 相互参照が必要などの状況が発生したら、とりあえずRcを使うことを検討するべき
- serde_jsonでタグによって種類が変わるjsonをうまく取り扱う方法
  - `#[serde(rename_all = "snake_case", tag = "type")]`
  - このディレクティブを付けたenumでタグによって種類が変わるjsonに対応できる
    - matchで綺麗に書ける
- Arcの中身にはSend + Sync対応(スレッドセーフ)なものしか入れられない
  - 例えばOnceCellはそれ自体はスレッドセーフではない
  - 代わりにOnceLockがスレッドセーフ版として存在するので使おうね  



### Rustの思想

借用チェッカーは、2つの役割を持っている

- メモリ安全性の確保。所有権を明確にすることで静的に検証可能なオブジェクトの自動解放を実現する
- データ一貫性の確保。可変参照を1度に1つしか作れないようにすることで複数の箇所からの競合する変更を防止する

後者の役割は前者役割を追求した結果の副産物として生まれているものであり、相互参照などで便利なデザインパターンを実現したい場合などはむしろ不便な要素となる

前者を維持しつつ後者の問題だけ回避するために`Rc<RefCell<>>`や`Arc<Mutex<>>`を用いる。

- Rc/Arc はいくらでもCloneすることができる(中身の所有権が移らない)ため、それをフィールドに持つ構造体もいくらでもCloneすることができる -> フィールドに渡して相互参照できる
- RefCell/Mutex はコンパイル時の借用チェックをスルーして内部可変性を提供する -> これを挟むことで中身の値を変更することができる

インナークラス`Arc<Mutex<Inner>>`を持つのが一般的なパターンと考えて良さそう。特に今回のような、メインで動くNodeとそれに渡すハンドラにそれぞれのデータフィールドがある場合

### Raft実装時の知見

- 入出力の取り扱いは複数タスクの並行処理で良いが、Raftの選挙状態やKVSも複数タスクで平行に取り扱うとレースコンディションの取り扱いが極端に難しくなる
- Raftステートマシンの選挙・投票リクエスト処理・値セット・値取得などの処理は単一のタスクで行い、入出力を担う複数タスクとチャンネルでやり取りするのが王道
  - コールバックのチャンネル(Message型)とRaftステートマシン(LinKvRequest型)をつなぐため、選挙開始のたびに型をつなぐ非同期タスクを1つspawnするのが良い
  - spawnした非同期タスクは選挙の終了時に終わってほしいので、CancellationTokenを発行して持たせる
- マジでawaitつけるの忘れがち


## tokioの使い方

- どこかでasyncを終端する必要がある。今回採用した方法はtokioのruntimeを生成して直接async関数の実行をブロックする方法

```rust
pub fn run(&mut self) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(self.serve());
}
```

- ↑の方法だと、run関数を持つNode構造体に依存してしまうことになる
  - run関数を呼び出す前段階で`tokio::spawn`を読んでもエラーになってしまう
  - main関数に`#[tokio::main]`を持たせると自動でblock_onするロジックを生成してくれるのでそれを使う方が見通しが良い
- async関数をtraitに持たせたい場合はasync_traitディレクティブをつける必要がある

## つくろうつよつよRaftロードマップ

- node.rsのリファクタリング
  - (DONE)Requestはワークロード依存なのでnode.rsから除き、各ファイルに分配する
  - (DONE)maelstrom-node mod に移す
  - (DONE)RPCErrorのファイルを分ける
  - (DONE)loggerを別ファイルに移す
  - (DONE)newとrunを分ける
    - (DONE)Node::new -> Raft::new(node) -> raft.start -> node.run の順番で実行したいので
    - (DONE)各mainをtokio::mainにする
  - (DONE)Initのやり方を変える
    - (DONE)ふつうはRaft::startの時にクラスタコンフィグを与えて開始したいはず
    - (DONE)Raft::startしてhandle_requestで非同期的にinitを受け取るんじゃなくて、initメッセージの処理はRaftHandlerの側で行う。クラスタコンフィグを受け取ったらそれを与えてRaftインスタンスを開始する
- RaftStateMachineの一般化
  - まずは既存のものをHashMapStateMachineにする
    - ログのコマンドはRaft本体では関知しないようにする(Vecu8にする)
- node.rsとraft.rsの依存性分離
  - Transportトレイトを定義してraftはTransportを持つようにする
  - RaftにTransportを渡して初期化、channelを生成。RaftHandlerにRaftのchannelを渡す。RaftHandlerはreceiveに関するつなぎ
    - send: Raft自身がTransportに送る
    - receive: Node -> RaftHandler -> Raftのchannelへ送信 という流れ
- RaftHandlerとRaftを分離する。LinKvRequestとRaftRequestを別にする
- RaftStorageの一般化
- read index最適化を入れる

