var express = require("express");
const exec = require('child_process').exec;
// const execSync = require('child_process').execSync;
var app = express();
var http = require('http').Server(app);

const port = process.env.PORT || 3000;


app.all('/:cmd/:filePath', (req, res) => {
    var cmd = req.params["cmd"]
    var filePath = req.params["filePath"].replace(/-/g, '/');
    const resultPath = filePath.split("/").reverse().slice(1).reverse().join("/"); // 親ディレクトリ
    var filename = filePath.split("/").reverse()[0].split('.')[0]; // ファイルの名前
    var extend = filePath.split("/").reverse()[0].split('.')[1]; // 拡張子
    if (cmd == `open`) {
        // ファイルを閲覧(open)
        openApp = exec(`open ${filePath}`, (err, stdout, stderr) => {
            if (err) {
                console.log(`stderr: ${stderr}`)
                return
            }
            console.log(`stdout: ${stdout}`)
        })
    } else if (cmd == `exec`) {
        // ファイルを実行(exec)
        // Python実行の時（とりあえずPythonだけ）
        if (extend == 'py') {
            var { PythonShell } = require('python-shell');
            var options = {
                // mode: 'text', // textもしくはjson
                pythonPath: '/Users/takeuchihiroki/.pyenv/versions/anaconda3-5.2.0/envs/Analysis/bin/python', // path/to/python
                pythonOptions: ['-u'],
                scriptPath: '/Users/takeuchihiroki/Google_drive_ecc/研究室/apache-tomcat-9.0.54/webapps/drawio/express_server_dir/', // path/to/express_dir/
            };
            var pyshell = new PythonShell(`${filePath}`, options);
            pyshell.send('hello');
            pyshell.on('message', function(message) {
                console.log(message);
            });
            // end the input stream and allow the process to exit
            pyshell.end(function(err, code, signal) {
                if (err) throw err;
                console.log('The exit code was: ' + code);
                console.log('The exit signal was: ' + signal);
                console.log('finished');
            });
        }
        // 他の言語を実行できるようにしたい時は、else ifで拡張子で分岐
        else {
            console.log('unknown language');
        }
    }
})

http.listen(port, () => {
    console.log('server listening. port:' + port);
})