package test

import (
	"fmt"
	"github.com/laincloud/backupd/crond"
	"time"
)

func init() {
	crond.Register("test", testf)
}

func testf(args crond.FuncArg) (crond.FuncResult, error) {
	fmt.Println(time.Now(), args.GetStringSlice("test", []string{}))
	return nil, nil
}
