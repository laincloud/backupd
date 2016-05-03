crond task
====

实现crond的各个任务.

该目录下的每个目录都是一个任务，每个任务中在`init()`中都会Register到crond上,因为在main.go中import时会自动载入.

也可以不放在init函数中，而是在main()函数中手动调用某个函数来完成初始化操作, 如backup功能.

我们可以假想成:

该目录下都是安装到crond上的一个个软件
