#!/usr/bin/env bash
#
# run_all.sh — Compila o projeto e dispara RunAll, que executa as 8 questões.
#
# Uso:
#   ./run_all.sh <caminho-do-dataset.csv>
#

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="$PROJECT_DIR/src"
BUILD_DIR="$PROJECT_DIR/build"
JAR_PATH="$PROJECT_DIR/pjbl01.jar"
OUTPUT_DIR="$PROJECT_DIR/output"

INPUT_DEFAULT="$PROJECT_DIR/operacoes_comerciais_inteira.csv"
INPUT="${1:-$INPUT_DEFAULT}"

if [[ ! -f "$INPUT" ]]; then
    echo "ERRO: dataset nao encontrado em '$INPUT'"
    echo "Uso: ./run_all.sh /caminho/para/transactions.csv"
    exit 1
fi

echo "==> Compilando..."
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"
javac -classpath "$(hadoop classpath)" -d "$BUILD_DIR" "$SRC_DIR"/*.java

echo "==> Empacotando $JAR_PATH..."
jar cf "$JAR_PATH" -C "$BUILD_DIR" .

echo "==> Executando RunAll..."
hadoop jar "$JAR_PATH" RunAll "$INPUT" "$OUTPUT_DIR"
