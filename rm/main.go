package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"

	"github.com/Fulim13/barrier-saga-demo/dao"
	"github.com/gin-gonic/gin"
)

// TransReq is a request to change one account's balance.
type TransReq struct {
	Account int `form:"account" binding:"gt=0,required"`
	Amount  int `form:"amount" binding:"required"`
}

func AdjustBalance(ctx *gin.Context) {
	var req TransReq
	if err := ctx.ShouldBind(&req); err != nil {
		log.Printf("failed to bind the request parameters: %s", err)
		ctx.String(http.StatusBadRequest, "failed to bind the request parameters")
		return
	}
	if err := dao.AdjustBalance(req.Account, req.Amount); err != nil {
		log.Printf("failed to adjust the balance of account %d: %s", req.Account, err)
		ctx.String(http.StatusInternalServerError, "failed to adjust the account balance")
		return
	}
	log.Printf("account %d, adjusted by %d", req.Account, req.Amount)
	ctx.String(http.StatusOK, "")
}

func main() {
	port := flag.Int("port", 1234, "http server port")
	flag.Parse()

	engine := gin.Default()
	engine.POST("/adjust_balance", AdjustBalance)
	engine.Run("127.0.0.1:" + strconv.Itoa(*port))
}
