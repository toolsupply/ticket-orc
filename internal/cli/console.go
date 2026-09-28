package cli

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

const (
	consoleMaxLine         = 4096
	consoleMaxArgs         = 64
	consolePrompt          = "ticket-orc> "
	consoleInterruptWindow = time.Second
	consoleTimestampLayout = "15:04:05"
)

// consoleRenderer serializes all console output. Asynchronous notices are
// placed on a fresh line and followed by a new prompt, leaving any text the
// terminal has echoed on the current line untouched.
type consoleRenderer struct {
	mu            sync.Mutex
	out           io.Writer
	err           io.Writer
	prompt        string
	promptVisible bool
	partialInput  string
	terminal      bool
}

type consoleRendererWriter struct {
	r     *consoleRenderer
	async bool
	err   bool
}

func newConsoleRenderer(out, err io.Writer) *consoleRenderer {
	terminal := false
	if foregroundTerminalEligible() {
		if file, ok := out.(*os.File); ok {
			if info, err := file.Stat(); err == nil {
				terminal = info.Mode()&os.ModeCharDevice != 0
			}
		}
	}
	return &consoleRenderer{out: out, err: err, prompt: consolePrompt, terminal: terminal}
}

func (renderer *consoleRenderer) writer() io.Writer {
	return consoleRendererWriter{r: renderer}
}

func (renderer *consoleRenderer) errorWriter() io.Writer {
	return consoleRendererWriter{r: renderer, err: true}
}

func (renderer *consoleRenderer) asyncWriter() io.Writer {
	return consoleRendererWriter{r: renderer, async: true}
}

func (renderer *consoleRenderer) promptLine() {
	renderer.mu.Lock()
	defer renderer.mu.Unlock()
	_, _ = io.WriteString(renderer.out, renderer.prompt+renderer.partialInput)
	renderer.promptVisible = true
}

func (renderer *consoleRenderer) commandStart() {
	renderer.mu.Lock()
	renderer.promptVisible = false
	renderer.partialInput = ""
	renderer.mu.Unlock()
}

func (renderer *consoleRenderer) setPartialInput(input string) {
	renderer.mu.Lock()
	renderer.partialInput = input
	renderer.mu.Unlock()
}

func (renderer *consoleRenderer) interruptPrompt(message string) {
	renderer.mu.Lock()
	defer renderer.mu.Unlock()
	if renderer.promptVisible {
		if renderer.terminal {
			_, _ = io.WriteString(renderer.out, "\r\x1b[2K")
		} else {
			_, _ = io.WriteString(renderer.out, "\r\n")
		}
	}
	_, _ = fmt.Fprintln(renderer.out, message)
	renderer.partialInput = ""
	_, _ = io.WriteString(renderer.out, renderer.prompt)
	renderer.promptVisible = true
}

func (writer consoleRendererWriter) Write(data []byte) (int, error) {
	if writer.r == nil {
		return len(data), nil
	}
	writer.r.mu.Lock()
	defer writer.r.mu.Unlock()
	target := writer.r.out
	if writer.err && writer.r.err != nil {
		target = writer.r.err
	}
	if writer.async && writer.r.promptVisible {
		if writer.r.terminal {
			if writer.r.partialInput != "" {
				if _, err := io.WriteString(writer.r.out, "\r\x1b[2K"); err != nil {
					return 0, err
				}
			} else if _, err := io.WriteString(writer.r.out, "\x1b[s\r\x1b[1L"); err != nil {
				return 0, err
			}
		} else if _, err := io.WriteString(writer.r.out, "\r\n"); err != nil {
			return 0, err
		}
	}
	n, err := target.Write(data)
	if err != nil {
		return n, err
	}
	if writer.async {
		if len(data) > 0 && data[len(data)-1] != '\n' {
			if _, err := io.WriteString(writer.r.out, "\n"); err != nil {
				return n, err
			}
		}
		if writer.r.promptVisible {
			if writer.r.terminal {
				if writer.r.partialInput != "" {
					if _, err := io.WriteString(writer.r.out, writer.r.prompt+writer.r.partialInput); err != nil {
						return n, err
					}
				} else if _, err := io.WriteString(writer.r.out, "\x1b[u\x1b[1B"); err != nil {
					return n, err
				}
			} else if _, err := io.WriteString(writer.r.out, writer.r.prompt); err != nil {
				return n, err
			}
		}
	}
	return n, nil
}
