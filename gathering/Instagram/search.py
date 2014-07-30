import urllib
import re
import hmac
from hashlib import sha256
import calendar

from instagram import oauth2
import simplejson

re_path_template = re.compile('{\w+}')

def encode_string(value):
    return value.encode('utf-8') \
                        if isinstance(value, unicode) else str(value)

def search_method(**config):

    class InstagramAPIMethod(object):

        path = config['path']
        method = config.get('method', 'GET')
        accepts_parameters = config.get("accepts_parameters", [])
        signature = config.get("signature", False)
        requires_target_user = config.get('requires_target_user', False)
        paginates = config.get('paginates', False)
        response_type = config.get("response_type", "list")
        include_secret = config.get("include_secret", False)
        objectify_response = config.get("objectify_response", True)

        def __init__(self, api, *args, **kwargs):
            self.api = api
            self.as_generator = kwargs.pop("as_generator", False)
            if self.as_generator:
                self.pagination_format = 'next_url'
            else:
                self.pagination_format = kwargs.pop('pagination_format', 'next_url')
            self.return_json = kwargs.pop("return_json", False)
            self.max_pages = kwargs.pop("max_pages", 3)
            self.with_next_url = kwargs.pop("with_next_url", None)
            self.parameters = {}
            self._build_parameters(args, kwargs)
            self._build_path()

        def _build_parameters(self, args, kwargs):
            for index, value in enumerate(args):
                if value is None:
                    continue
                try:
                    self.parameters[self.accepts_parameters[index]] = encode_string(value)
                except BaseException, e:
                    print "Too many arguments supplied"

            for key, value in kwargs.iteritems():
                if value is None:
                    continue
                if key in self.parameters:
                    raise Exception("Parameter %s already supplied" % key)
                self.parameters[key] = encode_string(value)
            if 'user_id' in self.accepts_parameters and not 'user_id' in self.parameters \
               and not self.requires_target_user:
                self.parameters['user_id'] = 'self'

        def _build_path(self):
            for variable in re_path_template.findall(self.path):
                name = variable.strip('{}')

                try:
                    value = urllib.quote(self.parameters[name])
                except Exception, e:
                    print 'No parameter value found for path variable: %s' % name
                del self.parameters[name]

                self.path = self.path.replace(variable, value)
            self.path = self.path + '.%s' % self.api.format

        def _build_pagination_info(self, content_obj):
            pagination = content_obj.get('pagination', {})
            if self.pagination_format == 'next_url':
                return pagination.get('next_url')
            if self.pagination_format == 'dict':
                return pagination

        def _do_api_request(self, url, method="GET", body=None, headers=None):
            headers = headers or {}
            if self.signature and self.api.client_ips != None and self.api.client_secret != None:
                secret = self.api.client_secret
                ips = self.api.client_ips
                signature = hmac.new(secret, ips, sha256).hexdigest()
                headers['X-Insta-Forwarded-For'] = '|'.join([ips, signature])

            response, content = oauth2.OAuth2Request(self.api).make_request(url, method=method, body=body, headers=headers)
            if response['status'] == '503' or response['status'] == '429':
                raise Exception(response['status'], "Rate limited", "Your client is making too many request per second")
            try:
                content_obj = simplejson.loads(content)
            except Exception, e:
                print 'Unable to parse response, not valid JSON.'

            if not content_obj.has_key('meta'):
                if content_obj.get('code') == 420 or content_obj.get('code') == 429:
                    error_message = content_obj.get('error_message') or "Your client is making too many request per second"
                    raise Exception(content_obj.get('code'), "Rate limited", error_message)
                raise Exception(content_obj.has_key('code'), content_obj.has_key('error_type'), content_obj.has_key('error_message'))
        
            api_responses = []
            status_code = content_obj['meta']['code']
            if status_code == 200:
                return content_obj["data"], self._build_pagination_info(content_obj)
            else:
                raise Exception(content_obj['meta']['error_type'], content_obj['meta']['error_message'])

        def _paginator_with_url(self, url, method="GET", body=None, headers=None):
            headers = headers or {}
            pages_read = 0
            while url and pages_read < self.max_pages:
                api_responses, url = self._do_api_request(url, method, body, headers)
                pages_read += 1
                yield api_responses, url
            return

        def _get_with_next_url(self, url, method="GET", body=None, headers=None):
            headers = headers or {}
            content, next = self._do_api_request(url, method, body, headers)
            return content, next

        def execute(self):
            url, method, body, headers = oauth2.OAuth2Request(self.api).prepare_request(self.method,
                                                                                 self.path,
                                                                                 self.parameters,
                                                                                 include_secret=self.include_secret)
            if self.with_next_url:
                return self._get_with_next_url(self.with_next_url, method, body, headers)
            if self.as_generator:
                return self._paginator_with_url(url, method, body, headers)
            else:
                content, next = self._do_api_request(url, method, body, headers)
            if self.paginates:
                return content
            else:
                return content

    def _call(api, *args, **kwargs):
        method = InstagramAPIMethod(api, *args, **kwargs)
        return method.execute()

    return _call

SUPPORTED_FORMATS = ['json']
class InstagramAPI(oauth2.OAuth2API):

    host = "api.instagram.com"
    base_path = "/v1"
    access_token_field = "access_token"
    authorize_url = "https://api.instagram.com/oauth/authorize"
    access_token_url = "https://api.instagram.com/oauth/access_token"
    protocol = "https"
    api_name = "Instagram"

    def __init__(self, *args, **kwargs):
        format = kwargs.get('format', 'json')
        if format in SUPPORTED_FORMATS:
            self.format = format
        else:
            raise Exception("Unsupported format")
        super(InstagramAPI, self).__init__(*args, **kwargs)

    media_search = search_method(
    			path="/media/search", 
    			accepts_parameters=['q', 'count', 'lat', 'lng', 'min_timestamp', 'max_timestamp', 'distance'])
    location_recent_media = search_method(
                path="/locations/{location_id}/media/recent",
                accepts_parameters=['count', 'max_id', 'location_id', 'min_timestamp', 'max_timestamp'],
                paginates=True)
    location_search = search_method(
                path="/locations/search",
                accepts_parameters=['q', 'count', 'lat', 'lng', 'foursquare_id', 'foursquare_v2_id', 'distance'])
