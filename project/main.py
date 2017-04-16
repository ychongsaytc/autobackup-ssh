#!/usr/bin/env python
# -*- coding: UTF-8 -*- 

import os
import sys
import time
import datetime
import json
import gzip
import stat
import shutil

import paramiko
from socksipy import socks

assert os.path.exists(sys.argv[1]), 'Configuration file does not exist.'

with open(sys.argv[1], 'r') as f:
	config = json.load(f)
	f.close()

assert ('output_dir' in config.keys()), 'Output directory was not set.'

config.setdefault('proxy', {
	'enabled': False,
})

config.setdefault('items', [])

if __name__ == '__main__':

	try:

		# set up socks5 tunnel
		if config['proxy']['enabled']:
			socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, config['proxy']['socks5']['host'], config['proxy']['socks5']['port'])
			paramiko.client.socket.socket = socks.socksocket

		for item in config['items']:

			assert ('id' in item.keys()), 'Item ID was not set.'

			print '==> Host: %s' % (item['id'])

			# output directory for current bucket
			output_dir = os.path.join(config['output_dir'], item['id'])

			# make directories recursively
			if not os.path.exists(output_dir):
				os.makedirs(output_dir)

			# initialize ssh client
			ch = paramiko.SSHClient()

			ch.set_missing_host_key_policy(paramiko.AutoAddPolicy())
			pkey = paramiko.RSAKey.from_private_key_file(item['private_key_path'])

			# establish ssh connection
			print '    SSH connecting to %s:%d...' % (item['host'], item['port'])
			ch.connect(hostname=item['host'], port=item['port'], username=item['username'], password=item['password'], pkey=pkey)
			sftp = ch.open_sftp()
			print '    SSH connected.'

			for entry in item['files']:

				assert ('id' in entry.keys()), 'Entry ID was not set.'

				print '    --> [files] %s' % (entry['id'])

				entry.setdefault('remote_path', None)

				# output directory for current entry
				output_dir_current = os.path.join(output_dir, 'files_'+entry['id'])

				# make directories recursively
				if not os.path.exists(output_dir_current):
					os.makedirs(output_dir_current)

				# traversing remote directory
				list = {
					'files': [],
					'dirs': [],
				}
				def sftp_walk_dir(path, base) :
					filenames = sftp.listdir(os.path.join(base, path))
					attrs = sftp.listdir_attr(os.path.join(base, path))
					for i in xrange(len(filenames)):
						path_current = os.path.join(path, filenames[i])
						obj = {
							'path': path_current,
							'st_mode': attrs[i].st_mode,
							'st_size': attrs[i].st_size,
							'st_atime': attrs[i].st_atime,
							'st_mtime': attrs[i].st_mtime,
						}
						if stat.S_ISDIR(attrs[i].st_mode):
							list['dirs'].append(obj)
							sftp_walk_dir(path_current, base)
						else:
							list['files'].append(obj)
				sftp_walk_dir('.', entry['remote_path'])

				print '        %d directories and %d files to process.' % (len(list['dirs']), len(list['files']))

				# make directories recursively
				for d in list['dirs']:
					if not os.path.exists(os.path.join(output_dir_current, d['path'])):
						os.makedirs(os.path.join(output_dir_current, d['path']))

				# download files
				for f in list['files']:
					should_pull = False
					path_local = os.path.join(output_dir_current, f['path'])
					if not os.path.exists(path_local): # local file does not exist
						should_pull = True
					elif f['st_mtime'] > os.stat(path_local).st_mtime: # local file is older than remote file
						should_pull = True
					elif f['st_size'] != os.stat(path_local).st_size: # file size does not match
						should_pull = True
					if should_pull:
						print '        Pulling %s' % (f['path'])
						sftp.get(os.path.join(entry['remote_path'], f['path']), os.path.join(output_dir_current, f['path']))

			for entry in item['mysql']:

				assert ('id' in entry.keys()), 'Entry ID was not set.'

				print '    ==> [mysql] %s' % (entry['id'])

				entry.setdefault('docker_container', None)
				entry.setdefault('host', None)
				entry.setdefault('user', None)
				entry.setdefault('password', None)
				entry.setdefault('exclude', [])
				entry.setdefault('days_to_keep', 0)

				# output directory for current entry
				output_dir_current = os.path.join(output_dir, 'mysql_'+entry['id'], datetime.datetime.now().strftime('%Y%m%d'))

				# make directories recursively
				if not os.path.exists(output_dir_current):
					os.makedirs(output_dir_current)

				# clean expired files
				if entry['days_to_keep'] > 0:
					now = int(time.time())
					for d in os.listdir(os.path.join(output_dir, 'mysql_'+entry['id'])):
						p = os.path.join(output_dir, 'mysql_'+entry['id'], d)
						if os.path.exists(p) and os.path.isdir(p):
							time_array = time.strptime(d, '%Y%m%d')
							time_stamp = int(time.mktime(time_array))
							if time_stamp < (now - 86400*entry['days_to_keep']):
								shutil.rmtree(p)
								print '        --> Clean expired files for MySQL: '+d

				# the MySQL Docker container
				if entry['docker_container']:
					command_prefix = 'docker exec '+entry['docker_container']+' '
				else:
					command_prefix = ''

				# list all databases
				stdin, stdout, stderr = ch.exec_command(command_prefix+'/usr/bin/mysql --host='+entry['host']+' --user='+entry['user']+' --password='+entry['password']+' --execute="show databases;"')
				dbs = stdout.read()
				dbs = dbs.split('\n')

				# filter
				for db in ['', 'Database', 'mysql', 'information_schema', 'performance_schema', 'sys']:
					if db in dbs:
						dbs.remove(db)
				for db in entry['exclude']:
					if db in dbs:
						dbs.remove(db)

				print '        %d databases to process.' % (len(dbs))

				for db in dbs:

					# export database
					print '        Pulling %s' % (db)
					stdin, stdout, stderr = ch.exec_command(command_prefix+'/usr/bin/mysqldump --host='+entry['host']+' --user='+entry['user']+' --password='+entry['password']+' '+db)

					# write to gzip
					filename = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')+'_'+db+'.sql'
					with gzip.open(os.path.join(output_dir_current, filename+'.gz'), 'wb') as f:
						f.write(stdout.read())
						f.close()

			# close ssh connection
			ch.close()
			print '    SSH connection closed.'

	except KeyboardInterrupt:

		if ch:
			ch.close()
			print '\nSSH connection closed.'

