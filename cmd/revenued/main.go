package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"go.sia.tech/host-revenue-api/api"
	"go.sia.tech/host-revenue-api/persist/sqlite"
	"go.sia.tech/host-revenue-api/stats"
	"go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	dir       string
	bootstrap bool

	logStdout bool
	logLevel  string

	gatewayAddr = ":9981"
	apiAddr     = ":9980"
)

func init() {
	flag.StringVar(&dir, "dir", "", "directory to store data")
	flag.StringVar(&gatewayAddr, "gateway", defaultGatewayAddr, "gateway address")
	flag.StringVar(&apiAddr, "api", defaultAPIAddr, "api address")
	flag.BoolVar(&bootstrap, "bootstrap", true, "bootstrap the network")
	flag.BoolVar(&logStdout, "log.stdout", true, "log to stdout")
	flag.StringVar(&logLevel, "log.level", "debug", "log level")
	flag.Parse()
}

func main() {
	// configure console logging note: this is configured before anything else
	// to have consistent logging. File logging will be added after the cli
	// flags and config is parsed
	consoleCfg := zap.NewProductionEncoderConfig()
	consoleCfg.TimeKey = "" // prevent duplicate timestamps
	consoleCfg.EncodeTime = zapcore.RFC3339TimeEncoder
	consoleCfg.EncodeDuration = zapcore.StringDurationEncoder
	consoleCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	consoleCfg.StacktraceKey = ""
	consoleCfg.CallerKey = ""
	consoleEncoder := zapcore.NewConsoleEncoder(consoleCfg)

	// only log info messages to console unless stdout logging is enabled
	consoleCore := zapcore.NewCore(consoleEncoder, zapcore.Lock(os.Stdout), zap.NewAtomicLevelAt(zap.InfoLevel))
	log := zap.New(consoleCore, zap.AddCaller())
	defer log.Sync()
	// redirect stdlib log to zap
	zap.RedirectStdLog(log.Named("stdlib"))

	if err := os.Mkdir(dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		panic(err)
	}

	// configure logging
	var level zap.AtomicLevel
	switch logLevel {
	case "debug":
		level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn":
		level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		log.Fatal("invalid log level", zap.String("level", logLevel))
	}

	// configure file logging
	fileCfg := zap.NewProductionEncoderConfig()
	fileEncoder := zapcore.NewJSONEncoder(fileCfg)

	fileWriter, closeFn, err := zap.Open(filepath.Join(dir, "log.log"))
	if err != nil {
		fmt.Println("failed to open log file:", err)
		os.Exit(1)
	}
	defer closeFn()

	// wrap the logger to log to both stdout and the log file
	log = log.WithOptions(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		// use a tee to log to both stdout and the log file
		return zapcore.NewTee(
			zapcore.NewCore(fileEncoder, zapcore.Lock(fileWriter), level),
			zapcore.NewCore(consoleEncoder, zapcore.Lock(os.Stdout), level),
		)
	}))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		time.Sleep(10 * time.Second)
		os.Exit(-1)
	}()

	apiListener, err := net.Listen("tcp", apiAddr)
	if err != nil {
		log.Panic("failed to listen on api address", zap.Error(err))
	}
	defer apiListener.Close()

	// start the gateway
	g, err := gateway.New(gatewayAddr, bootstrap, filepath.Join(dir, "gateway"))
	if err != nil {
		log.Panic("failed to create gateway", zap.Error(err))
	}
	defer g.Close()

	// start the consensus set
	cs, errCh := consensus.New(g, bootstrap, filepath.Join(dir, "consensus"))
	select {
	case err := <-errCh:
		if err != nil {
			log.Panic("failed to create consensus", zap.Error(err))
		}
	default:
		go func() {
			if err := <-errCh; err != nil && !strings.Contains(err.Error(), "ThreadGroup already stopped") {
				log.Panic("failed to initialize consensus", zap.Error(err))
			}
		}()
	}
	defer cs.Close()

	// start the transaction pool
	tp, err := transactionpool.New(cs, g, filepath.Join(dir, "tpool"))
	if err != nil {
		log.Panic("failed to create transaction pool", zap.Error(err))
	}
	defer tp.Close()

	db, err := sqlite.OpenDatabase(filepath.Join(dir, "revenue.sqlite3"), log.Named("sqlite3"))
	if err != nil {
		log.Panic("failed to open database", zap.Error(err))
	}
	defer db.Close()

	go syncMarketData(ctx, db, log.Named("marketSync"))

	lastChange, err := db.LastChange()
	if err != nil {
		log.Panic("failed to get last change", zap.Error(err))
	}

	go func() {
		if err := cs.ConsensusSetSubscribe(db, lastChange, ctx.Done()); err != nil && !strings.Contains(err.Error(), "ThreadGroup already stopped") {
			log.Panic("failed to subscribe to consensus set", zap.Error(err))
		}
	}()

	// create a subscriber
	sp, err := stats.NewProvider(db, log.Named("stats"))
	if err != nil {
		log.Panic("failed to create stats provider", zap.Error(err))
	}

	api := http.Server{
		Handler:     api.NewServer(sp, log.Named("api")),
		ReadTimeout: 30 * time.Second,
	}
	defer api.Close()

	go func() {
		err := api.Serve(apiListener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Panic("failed to serve api", zap.Error(err))
		}
	}()

	// wait for the context to be canceled
	<-ctx.Done()
}
