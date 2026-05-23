set -e
rm litmus_real.txt >/dev/null 2>&1
asciinema rec litmus_real.txt --command 'cat litmus/results/litmus.log' --headless --window-size 1000x1000 >/dev/null 2>&1
diff -u999 litmus_mock.txt litmus_real.txt