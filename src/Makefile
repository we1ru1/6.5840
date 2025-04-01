# 目录定义
SRC_DIR = $(shell pwd)
MAIN_DIR = $(SRC_DIR)/main
MRAPPS_DIR = $(SRC_DIR)/mrapps
PG_DIR = $(SRC_DIR)/main
LOG_DIR = $(MAIN_DIR)/logs


# 默认插件文件 (可在命令行覆盖)
PLUGIN_FILE ?= wc.go
PLUGIN_NAME = $(basename $(PLUGIN_FILE)).so

# 默认输入文件（可在命令行覆盖）
PG_FILE ?= pg-being_ernest.txt

# 插件和程序
WC_PLUGIN = $(MRAPPS_DIR)/$(PLUGIN_NAME)
MR_COORDINATOR = $(MAIN_DIR)/mrcoordinator.go
MR_SEQUENTIAL = $(MAIN_DIR)/mrsequential.go
WORKER = $(MAIN_DIR)/mrworker.go

# 构建和运行命令
GO = go
GO_BUILD = $(GO) build
GO_RUN = $(GO) run

# 默认目标：编译插件
.PHONY: all build plugin coordinator worker clean kill

all: build

# 构建所有组件
build: test

# 时间格式变量
TIME_FORMAT = "+%Y-%m-%d_%H-%M-%S"

# 编译测试功能
test: plugin sequential coordinator workers


# 编译插件
plugin:
	@echo "编译 Map/Reduce 插件: $(PLUGIN_FILE)..."
	cd $(MRAPPS_DIR) && $(GO_BUILD) -buildmode=plugin $(PLUGIN_FILE)
	@echo "插件已编译: $(WC_PLUGIN)"

# 运行协调器（后台运行，输出重定向到日志文件）
coordinator:
	@echo "启动 Coordinator..."
	cd $(MAIN_DIR) && $(GO_RUN) $(MR_COORDINATOR) $(shell ls $(PG_DIR)/$(PG_FILE)) > $(LOG_DIR)/coordinator.log 2>&1 &
	@echo "Coordinator 已启动（查看 main/mr_coordinator.log 获取日志）"

sequential:
	@cd $(MAIN_DIR) && $(GO_RUN) $(MR_SEQUENTIAL) $(WC_PLUGIN) $(shell ls $(PG_DIR)/$(PG_FILE)) > $(LOG_DIR)/sequential.log 2>&1 
	@sort $(MAIN_DIR)/mr-out-0 > $(MAIN_DIR)/mr-correct-wc.txt
	@cd $(MAIN_DIR) && rm -f mr-out*

# 运行工作器（可多次执行启动多个工作器）
worker:
	@echo "启动 Worker..."
	cd $(MAIN_DIR) && $(GO_RUN) $(WORKER) $(WC_PLUGIN) > $(LOG_DIR)/worker_$$(date $(TIME_FORMAT)).log 2>&1
	@echo "Worker 已启动"
	@cd $(MAIN_DIR) && sort mr-out* | grep . > mr-wc-all
	@cd $(MAIN_DIR) && rm mr-out*


# 多工作器（一次启动3个）
workers: 
	@(cd $(MAIN_DIR) && $(GO_RUN) $(WORKER) $(WC_PLUGIN) > $(LOG_DIR)/worker_A_$$(date $(TIME_FORMAT)).log 2>&1 &)
	@(cd $(MAIN_DIR) && $(GO_RUN) $(WORKER) $(WC_PLUGIN) > $(LOG_DIR)/worker_B_$$(date $(TIME_FORMAT)).log 2>&1 &)
	@(cd $(MAIN_DIR) && $(GO_RUN) $(WORKER) $(WC_PLUGIN) > $(LOG_DIR)/worker_C_$$(date $(TIME_FORMAT)).log 2>&1 )
	@echo "3个 Worker 已启动"
	@cd $(MAIN_DIR) && sort mr-out* | grep . > mr-wc-all
	@cd $(MAIN_DIR) && rm mr-out*



# 终止所有 MapReduce 相关进程
kill:
	@echo "终止所有 MapReduce 进程..."
	@-pkill -9 -f mrcoordinator || true
	@-pkill -9 -f mrworker || true
	@-pkill -9 -f "mr-" || true  # 确保匹配所有相关进程
	@echo "进程已终止"

# 清理生成的文件
clean:
	@echo "清理所有生成的文件..."
	@rm -f $(MRAPPS_DIR)/*.so
	@rm -f $(LOG_DIR)/*.log
	@rm -f $(MAIN_DIR)/mr-*-*
	@rm -f $(MAIN_DIR)/mr-out-*
	@echo "清理完成"

# 完整重启：终止、清理、构建、启动
restart: clean build
	@echo "系统已完全重启"


## 编译插件
# make plugin

## 启动协调器（后台运行）
#make coordinator

## 启动工作器（可多次运行以启动多个工作器）
#make worker

## 或者一次启动3个工作器
#make workers

## 终止所有相关进程
#make kill

## 清理所有生成文件
#make clean

## 完整重启（终止、清理、构建、启动协调器和3个工作器）
#make restart

