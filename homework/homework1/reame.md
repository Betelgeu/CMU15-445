# autograde.py
在[Shao Chun大佬](https://github.com/GODZAOZAO/cmu15-445-homework/tree/main)提供的判题器的基础上做了一点小的修改
判题器的原理其实就是把你的sql执行结果和ans文件夹中的sql文件做对比。
改动加强了一点判题器的鲁棒性
因为Shao Chun大佬的q5～q10使用的是duckdb, 而我使用sqlite编写，所以对判题器做了一点小改变，让他在找不到对应.sqlite.sql的时候，搜索ans中是否有对应的.duckdn.sql作为答案参考。

# ans
这里的ans中放了大佬编写的sql，最外层放的是我自己的sql代码。
经过测试结果是一致的。