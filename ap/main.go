package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Fulim13/barrier-saga-demo/dao"
	"github.com/gin-gonic/gin"
	"github.com/rs/xid"
)

const (
	// Money is moved between an account at bank A and an account at bank B.
	bankAURL         = "http://localhost:1234"
	bankBURL         = "http://localhost:5678"
	transOutBranchID = "trans_out_branchid"
	transInBranchID  = "trans_in_branchid"
	forward          = "action"
	backward         = "compensate"
)

type TransReq struct {
	FromUid int `form:"from" binding:"gt=0,required"`   // account the money leaves
	ToUid   int `form:"to" binding:"gt=0,required"`     // account the money arrives in
	Amount  int `form:"amount" binding:"gt=0,required"` // amount to move
}

// postForm sends a form-encoded POST request.
func postForm(reqURL string, account, amount int) error {
	data := url.Values{"account": []string{strconv.Itoa(account)}, "amount": []string{strconv.Itoa(amount)}}
	request, err := http.NewRequest(http.MethodPost, reqURL, strings.NewReader(data.Encode()))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := http.Client{
		Timeout: time.Second,
	}
	resp, err := client.Do(request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	bs, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return errors.New(string(bs))
	}
	return nil
}

func barrier(ctx *gin.Context, tx *sql.Tx, branchID string, opName string) bool {
	return Barrier(tx, ctx.GetString("gid"), branchID, opName)
}

// Barrier inserts "<gid>-<branchID>-<op>" into the barrier table, which has a
// unique index on that column.
func Barrier(tx *sql.Tx, gid, branchID, op string) bool {
	// Different keys never interfere with each other. For the same key, one
	// transaction blocks the other until the first one finishes -- that wait is
	// what makes an in-flight branch and its compensation see a consistent view.
	result, err := tx.Exec(
		"insert into barrier (op) values ($1) on conflict (op) do nothing",
		fmt.Sprintf("%s-%s-%s", gid, branchID, op))
	affected, err := result.RowsAffected()
	if err != nil {
		log.Printf("cannot read the affected row count: %s", err)
		return false
	}
	return affected > 0
}

// transOut moves money out of the account at bank A.
func transOut(ctx *gin.Context, req *TransReq) error {
	tx, err := dao.GetDB().BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	// Idempotency control and With transOutComp it also guarantee anti-hanging
	// (1) Even before a transaction has committed or rolled back, if another transaction tries to write the same `gid-branchid-op`,
	// it will block until the first transaction commits or rolls back.
	if !barrier(ctx, tx, transOutBranchID, forward) {
		tx.Commit() // commit or rollback, either is fine: the insert did nothing anyway
		// The barrier stopped us, so this branch already ran and store in db. Report success.
		return nil
	}

	log.Printf("about to transfer out")
	err = postForm(bankAURL+"/adjust_balance", req.FromUid, -req.Amount)
	if err == nil {
		log.Printf("transfer out succeeded")
		return tx.Commit()
	}
	log.Printf("transfer out failed")
	tx.Rollback()
	return err
}

// transOutComp compensates the transfer out.
func transOutComp(ctx *gin.Context, req *TransReq) {
	tx, err := dao.GetDB().BeginTx(context.Background(), nil)
	if err != nil {
		log.Printf("cannot start the compensation transaction: %s", err)
		return
	}
	defer tx.Commit()
	// Idempotency control.
	if !barrier(ctx, tx, transOutBranchID, backward) {
		return
	}
	// Empty-compensation control: if the forward key can still be inserted, the
	// forward branch never committed, so there is nothing to compensate.
	// (2) Even before a transaction has committed or rolled back, if another transaction tries to write the same `gid-branchid-op`,
	// it will block until the first transaction commits or rolls back.
	if barrier(ctx, tx, transOutBranchID, forward) {
		log.Println("the branch has not run yet, no compensation needed")
		return
	}

	// Else Need to do Compensation
	log.Printf("about to compensate the transfer out")
	// Keep retrying until it finally succeeds.
	// Compensation operations must ultimately succeed.
	// If they don't succeed for the moment,
	// they are retried repeatedly until they finally return success.
	// In practice, failures need to be monitored: once the failure count exceeds a threshold,
	// an alert must fire and an operator must step in to resolve the problem.
	for {
		if postForm(bankAURL+"/adjust_balance", req.FromUid, req.Amount) == nil {
			break
		}
		// Once the failure count crosses a threshold this should raise an alert
		// so that a human operator can step in.
		log.Printf("compensating the transfer out failed, retrying")
		time.Sleep(5 * time.Second)
	}
	log.Printf("compensating the transfer out succeeded")
}

// transIn moves money into the account at bank B.
func transIn(ctx *gin.Context, req *TransReq) error {
	tx, err := dao.GetDB().BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	// Idempotency control, and at the same time protection against a dangling action.
	if !barrier(ctx, tx, transInBranchID, forward) {
		tx.Commit()
		// The barrier stopped us, so this branch already ran. Report success.
		return nil
	}

	log.Printf("about to transfer in")
	err = postForm(bankBURL+"/adjust_balance", req.ToUid, req.Amount)
	if err == nil {
		log.Printf("transfer in succeeded")
		return tx.Commit()
	}
	log.Printf("transfer in failed")
	tx.Rollback()
	return err
}

// transInComp compensates the transfer in.
func transInComp(ctx *gin.Context, req *TransReq) {
	tx, err := dao.GetDB().BeginTx(context.Background(), nil)
	if err != nil {
		log.Printf("cannot start the compensation transaction: %s", err)
		return
	}
	defer tx.Commit()
	// Idempotency control.
	if !barrier(ctx, tx, transInBranchID, backward) {
		return
	}
	// Empty-compensation control: if the forward key can still be inserted, the
	// forward branch never committed, so there is nothing to compensate.
	if barrier(ctx, tx, transInBranchID, forward) {
		log.Println("the branch has not run yet, no compensation needed")
		return
	}

	log.Printf("about to compensate the transfer in")
	// Keep retrying until it finally succeeds.
	for {
		if postForm(bankBURL+"/adjust_balance", req.ToUid, -req.Amount) == nil {
			break
		}
		// Once the failure count crosses a threshold this should raise an alert
		// so that a human operator can step in.
		log.Printf("compensating the transfer in failed, retrying")
		time.Sleep(5 * time.Second)
	}
	log.Printf("compensating the transfer in succeeded")
}

func Transfer(ctx *gin.Context) {
	var req TransReq
	if err := ctx.ShouldBind(&req); err != nil {
		log.Printf("failed to bind the request parameters: %s", err)
		ctx.String(http.StatusBadRequest, "failed to bind the request parameters")
		return
	}

	var (
		mu    sync.Mutex
		txErr error
	)
	fail := func(err error) {
		mu.Lock()
		if txErr == nil {
			txErr = err
		}
		mu.Unlock()
		// One (or more) branches failed, so run every compensation. They could
		// also run in parallel; for a transfer the order of the two does not matter.
		transInComp(ctx, &req)
		transOutComp(ctx, &req)
	}

	wg := sync.WaitGroup{}
	wg.Go(func() {
		if err := transOut(ctx, &req); err != nil {
			log.Printf("transfer out failed: %s", err)
			fail(err)
		}
	})
	wg.Go(func() {
		if err := transIn(ctx, &req); err != nil {
			log.Printf("transfer in failed: %s", err)
			fail(err)
		}
	})
	wg.Wait()

	if txErr != nil {
		ctx.String(http.StatusInternalServerError, txErr.Error())
		return
	}
	ctx.String(http.StatusOK, "ok")
}

func GidMW(ctx *gin.Context) {
	ctx.Set("gid", xid.New().String())
	ctx.Next()
}

func main() {
	engine := gin.Default()
	engine.Use(GidMW) // global middleware that assigns every request a fresh global transaction id.
	engine.POST("/transfer", Transfer)
	engine.Run("127.0.0.1:7001")
}
