#!/bin/bash
pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" /tmp/dvdrental.tar