### Service Manager
systemd and systemctl allow you to add LEDbelly to the systems service manager. This enables LEDbelly to be reloaded if it crashes, disconnects or your system reboots, helping to keep your Live Events working in real-time.


1) Edit `sudo nano /etc/systemd/system/ledbelly.service`

	with the following, adjusting your installation path `/canvas/live-events/`
```
[Unit]
Description=LEDbelly - Live Events Daemon
Requires=network.target
After=syslog.target network-online.target

[Service]
Type=forking
WorkingDirectory=/canvas/live-events
ExecStart=/usr/bin/bash -lc 'bundle exec shoryuken -r ./ledbelly -C cfg/sqs.yml -L /dev/null -d'
PIDFile=/canvas/live-events/log/shoryuken.pid
Restart=on-failure
RestartSec=1800
KillMode=process

[Install]
WantedBy=multi-user.target
```

2) Reload and Enable

	`sudo systemctl daemon-reload; sudo systemctl enable ledbelly`

3) Start the service

	`sudo systemctl start ledbelly`

4) Check the status

	`sudo systemctl status ledbelly.service`

5) Stop and Restart

	`sudo systemctl stop ledbelly`

	`sudo systemctl restart ledbelly`