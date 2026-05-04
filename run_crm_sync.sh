#!/bin/bash
cd /root/Insulleads
exec /usr/bin/python3 utils/crm_sync.py "$@"
