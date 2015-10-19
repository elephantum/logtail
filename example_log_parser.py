import logtail
import socket
import re
import os

LOG_FILENAME = os.environ.get('LOG_FILENAME', '/var/log/rm-release/nginx-access.log')
LOG_FIELDS = {
    "time_local": logtail.nginx_time,
    "request": str,
    "msec": float,
    "status": int,
    "body_bytes_sent": int,
    "request_time": float,
    "remote_addr": str,
    "remote_user": str,
    "http_host": str,
    "http_referer": str,
    "http_user_agent": str,
    "http_x_forwarded_for": str,
    "upstream_addr": str,
    "upstream_http_host": str,
    "upstream_response_time": float,
    "upstream_cache_status": str
}
GRAPHITE_HOST = os.environ.get('GRAPHITE_HOST')
ENV = os.environ.get('ENV', 'prod')

parts_re = re.compile(r'(\w+) /?(/[^ ]*) .*')
url_1part_re = re.compile(r'(/[^/]+).*')
url_2parts_re = re.compile(r'(/[^/]+(/[^/?]+)?).*')


def prepare_data(data):
    data['service'] = None
    data['service'][data['upstream_addr'] == '-'] = 'static'
    data['service'][data['upstream_addr'] == '127.0.0.1:4091'] = 'backend'
    data['service'][data['upstream_addr'] == '127.0.0.1:4092'] = 'screenshot'
    data['service'][data['upstream_addr'] == '127.0.0.1:9000'] = 'frontend'

    data['backend_module'] = data['request'].apply(guess_backend_module)

    data['request_time_xx'] = '-'
    data['request_time_xx'][data['request_time'].between(0, 0.5)] = '0-500ms'
    data['request_time_xx'][data['request_time'].between(0.5, 1)] = '500ms-1s'
    data['request_time_xx'][data['request_time'] > 1] = '1s-inf'

    data['status_xx'] = '-'
    data['status_xx'][data['status'].between(100,199)] = '1xx'
    data['status_xx'][data['status'].between(200,299)] = '2xx'
    data['status_xx'][data['status'].between(300,399)] = '3xx'
    data['status_xx'][data['status'].between(400,499)] = '4xx'
    data['status_xx'][data['status'].between(500,599)] = '5xx'

    data['domain'] = 'custom'
    data['domain'][data['http_host'] == 'readymag.com'] = 'readymag'
    data['domain'][data['http_host'] == 'embed.readymag.com'] = 'readymag'
    data['domain'][data['http_host'] == 'readymag.local'] = 'readymag'
    data['domain'][data['http_host'].str.endswith(':8080')] = 'readymag'

    return data


def main(plugin):
    data = plugin.read_data()

    data = prepare_data(data)

    def aggregate_qps(pattern, g):
        for k, v in g.size().iterkv():
            if not isinstance(k, tuple): k = (k,)
            plugin.send_qps('{0}.qps'.format(pattern.format(*k)), v)

    def aggregate_latency(pattern, g):
        for k, v in g['request_time'].quantile(0.75).iterkv():
            if not isinstance(k, tuple): k = (k,)
            plugin.send('{0}.latency_p75'.format(pattern.format(*k)), v)

        for k, v in g['request_time'].quantile(0.95).iterkv():
            if not isinstance(k, tuple): k = (k,)
            plugin.send('{0}.latency_p95'.format(pattern.format(*k)), v)

        for k, v in g['upstream_response_time'].quantile(0.75).iterkv():
            if not isinstance(k, tuple): k = (k,)
            plugin.send('{0}.upstream_latency_p75'.format(pattern.format(*k)), v)

        for k, v in g['upstream_response_time'].quantile(0.95).iterkv():
            if not isinstance(k, tuple): k = (k,)
            plugin.send('{0}.upstream_latency_p95'.format(pattern.format(*k)), v)

    aggregate_qps('total', data.groupby([lambda x: 'total']))
    aggregate_latency('total', data.groupby([lambda x: 'total']))

    aggregate_qps('total.latency_xx.{1}', data.groupby([lambda x: 'total', 'request_time_xx']))

    aggregate_qps('total.status_xx.{1}', data.groupby([lambda x: 'total', 'status_xx']))

    aggregate_qps('total.cache_status.{1}', data.groupby([lambda x: 'total', 'upstream_cache_status']))

    aggregate_qps('service.{0}', data.groupby(['service']))
    aggregate_latency('service.{0}', data.groupby(['service']))

    aggregate_qps('service.{0}.cache_status.{1}', data.groupby(['service', 'upstream_cache_status']))

    aggregate_qps('service.{0}.latency_xx.{1}', data.groupby(['service', 'request_time_xx']))

    aggregate_qps('domain.{0}', data.groupby(['domain']))
    aggregate_latency('domain.{0}', data.groupby(['domain']))
    aggregate_qps('domain.{0}.latency_xx.{1}', data.groupby(['domain', 'request_time_xx']))
    aggregate_qps('domain.{0}.cache_status.{1}', data.groupby(['domain', 'upstream_cache_status']))


if __name__ == '__main__':
    import schedule
    import time
    import logging

    plugin = logtail.Plugin(
        state_filename='/tmp/app/state.json',
        log_filename=LOG_FILENAME,
        log_fields=LOG_FIELDS,
        graphite_host=GRAPHITE_HOST,
        graphite_prefix="env.{0}.host.{1}.rm-web".format(ENV, socket.gethostname())
    )

    if not plugin.test_mode:
        def main_safe():
            try:
                print 'run iteration'

                main(plugin)
            except:
                logging.exception('hmmm')

        schedule.every().minute.do(main_safe)

        print 'start loop'
        while True:
            schedule.run_pending()
            time.sleep(1)

    else:
        main(plugin)
