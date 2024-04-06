# test

## about

database by hirokihello

## databaseのデータ永続化について

ファイルシステムを使う。
しかしその中でも、ブロックとページングを使い高速化をする。


# 制作ログ
## day 1
### やったこと
- chapter 1 読了
- chapter 2 読了
### 成果
- chapter 1 理解
- chapter 2 理解
### 学んだこと
#### トランザクション処理の不具合により生まれるバグ名について理解
- repeatable read
トランザクションの内部では常に READ した場合、同じ値が返ってくると想定すること

下記はこの原則が守られていないことでバグになる例
```疑似コード
// money = 100 の student がいるとする
// T1 と T2 が並行で実行される
// 本来は 190 になるのが正しい

#T1 money から 10 を引く
const res1 = select money from students;
print res1; // => 100;
// 下記の実行中に T2 が実行される
update students set money = money - 10; // money 100 が読み込まれて、書き込みが行われるので money が 90 がセットされる
print res1; // => 90;

#T2 money に 100 を足す
update students set money = res1 + 100; // money に 100 を足す処理を行う

// 二つとも終わった後の値
const res1 = select money from students;
print res1; // => 90;
```

- phantom record
update 処理を行う前に想定していなかったレコードが存在すること
```疑似コード
#T1
const res1 = select * from students;
print res1; // この時点では、xxx_additional の id を持つ奴はいない
// ここで T2 が実行される
update students set id = id+2; // xxx_additional の id を持つ生徒も更新されてしまう

# T2
insert students id = "xxx_additional";

```

# chap5
recovery manager は transaction ごとに生成される