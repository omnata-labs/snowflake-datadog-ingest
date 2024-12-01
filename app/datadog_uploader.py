import requests
from requests.adapters import HTTPAdapter
import json
import _snowflake
from typing import List,Dict,Literal
from datadog_proto.agent_payload_pb2 import AgentPayload
from datadog_proto.tracer_payload_pb2 import TracerPayload, TraceChunk
from datadog_proto.span_pb2 import Span
from urllib3.util import Retry


def translate_trace_id(trace_id):
    if trace_id is None:
      return 0
    try:
      return int(str(trace_id), 16) & 0xFFFFFFFFFFFFFFFF
    except:
      raise ValueError(f"Invalid trace_id: {trace_id}")

def translate_span_id(span_id):
    if span_id is None:
      return 0
    try:
      return int(str(span_id), 16)
    except:
      raise ValueError(f"Invalid span_id: {span_id}")

class DatadogUploader:
    def __init__(self):
      self._log_messages:List[Dict]=[]
      self._span_messages:List[Dict]=[]
      self.api_key = _snowflake.get_generic_secret_string('api_key')
      retry_strategy = Retry(
        total=4,  # maximum number of retries
        status_forcelist=[403, 429, 500, 502, 503, 504],  # the HTTP status codes to retry on
      )
      adapter = HTTPAdapter(max_retries=retry_strategy)
      # create a new session object
      self._session = requests.Session()
      self._session.mount("http://", adapter)
      self._session.mount("https://", adapter)


    def process(self, record_type:Literal['SPAN','SPAN_EVENT','LOG'],payload:Dict):
      if record_type in ['SPAN','SPAN_EVENT']:
        self._span_messages.append(payload)
      elif record_type == 'LOG':
        self._log_messages.append(payload)
      elif record_type == 'METRIC':
        pass
      else:
        raise ValueError(f"Unknown record type: {record_type}")

      if len(self._span_messages) >= 100:
        yield from self.do_span_messages_upload()
      if len(self._log_messages) >= 100:
        yield from self.do_log_messages_upload()

    def do_log_messages_upload(self):
      if len(self._log_messages) > 0:
        # We use a logger which automatically adds trace and span IDs to the log messages
        # We need to take these, convert them to the datadog format (ints), and add them to the expected location for log-trace correlation
        # https://docs.datadoghq.com/tracing/other_telemetry/connect_logs_and_traces/python/
        for log_message in self._log_messages:
          if 'TRACE' in log_message:
            if 'trace_id' in log_message['TRACE']:
              log_message['trace_id'] = translate_trace_id(log_message['TRACE']['trace_id'])
            if 'span_id' in log_message['TRACE']:
              log_message['span_id'] = translate_span_id(log_message['TRACE']['span_id'])
          
          if 'RECORD_ATTRIBUTES' in log_message:
            if 'trace_id' in log_message['RECORD_ATTRIBUTES']:
              log_message['trace_id'] = translate_trace_id(log_message['RECORD_ATTRIBUTES']['trace_id'])
            if 'span_id' in log_message['RECORD_ATTRIBUTES']:
              log_message['span_id'] = translate_span_id(log_message['RECORD_ATTRIBUTES']['span_id'])
          
          if 'trace_id' in log_message:
            log_message['dd.trace_id'] = str(log_message['trace_id'])
          if 'span_id' in log_message:
            log_message['dd.span_id'] = str(log_message['span_id'])

          log_message['service'] = log_message.get('service', 'unknown')
          log_message['message'] = log_message.get('VALUE', None)
          del log_message['VALUE']
          log_message['status'] = log_message.get('RECORD', {}).get('severity_text', None)
          if log_message.get('status', None) in ['ERROR','CRITICAL']:
            log_message['error.message'] = log_message.get('message', None)
          
          if 'RECORD_ATTRIBUTES' in log_message:
            if 'exception.message' in log_message['RECORD_ATTRIBUTES']:
              log_message['error.stack'] = log_message['RECORD_ATTRIBUTES']['exception.message']
        response = self._session.post(
          "https://http-intake.logs.us3.datadoghq.com/api/v2/logs",
          json=self._log_messages,
          headers={
            "DD-API-KEY":self.api_key
          })
        response.raise_for_status()    
        self._log_messages=[]
        yield (response.text,)

    def do_span_messages_upload(self,):
      if len(self._span_messages) > 0:
        
        # we have to restructure the Snowflake span records, and also convert all the trace IDs to datadog format (ints)
        # Snowflake SPAN records look like this:
        # RECORD column:
        # {
        #   "kind": "SPAN_KIND_INTERNAL",
        #   "name": "invoke_plugin",
        #   "parent_span_id": "d64c3ec6b8aafb16",
        #   "status": {
        #     "code": "STATUS_CODE_UNSET"
        #   }
        # }
        # TRACE column:
        # {
        #   "span_id": "268a33e0301d1b78",
        #   "trace_id": "01b887a035284edeab312449ad9c603a"
        # }
        # SCOPE column:
        # {
        #   "name": "omnata_plugin_runtime"
        # }
        # TIMESTAMP:
        # 2024-11-21 21:59:50.616
        # RECORD_ATTRIBUTES:
        # {
        #    "stream_name": "issues"
        # }
        # (they don't all look like that, but that's the general idea)
        # RESOURCE_ATTRIBUTES:
        # {
        #   "db.user": "SYSTEM",
        #   "snow.application.id": 16756,
        #   "snow.application.name": "PLUGIN_JIRA_APP_DEV_JAMES",
        #   "snow.database.id": 16756,
        #   "snow.database.name": "PLUGIN_JIRA_APP_DEV_JAMES",
        #   "snow.executable.id": 996111,
        #   "snow.executable.name": "SYNC(SYNC_REQUEST OBJECT):OBJECT",
        #   "snow.executable.runtime.version": "3.10",
        #   "snow.executable.type": "PROCEDURE",
        #   "snow.owner.id": 44167,
        #   "snow.owner.name": "PLUGIN_JIRA_APP_DEV_JAMES",
        #   "snow.patch": 29,
        #   "snow.query.id": "01b8871c-3203-d84a-0000-81c10608d0d6",
        #   "snow.schema.id": 306220,
        #   "snow.schema.name": "PLUGIN",
        #   "snow.session.id": 142665956208686,
        #   "snow.session.role.primary.id": 84360,
        #   "snow.session.role.primary.name": "OMNATA_APP_JAMES",
        #   "snow.user.id": 0,
        #   "snow.version": "DEVELOPMENT",
        #   "snow.warehouse.id": 1,
        #   "snow.warehouse.name": "COMPUTE_WH",
        #   "telemetry.sdk.language": "python"
        # }
        # spans are grouped by trace_id
        trace_chunks:List[TraceChunk] = []
        tracer_payload = TracerPayload()
        #tracer_payload.container_i_d = "835a9719-1797-400d-acb9-c8090ea7a487" #uuid.uuid4().urn.removeprefix("urn:uuid:")
        #tracer_payload.language_name = "python"
        #tracer_payload.language_version = "3.10.9"
        #tracer_payload.tracer_version = "2.17.0"
        tracer_payload.env = "production"
        #tracer_payload.app_version = "1"
        
        traces:Dict[int,List[Dict]] = {}
        for span_message in self._span_messages:
          trace_id = translate_trace_id(span_message.get('TRACE',{}).get('trace_id', '0'))
          if trace_id not in traces:
            traces[trace_id] = []
          span_id = translate_span_id(span_message.get('TRACE',{}).get('span_id', '0'))
          traces[trace_id].append({
            "service": span_message.get('service', 'unknown'),
            "name": span_message.get('SCOPE',{}).get('name', 'unknown'),
            "resource": span_message.get('RECORD',{}).get('name', 'unknown'),
            "trace_id": trace_id,
            "span_id": span_id,
            "parent_id": translate_span_id(span_message.get('RECORD',{}).get('parent_span_id', 0)),
            "start": span_message.get('date', 0),
            "duration": span_message.get('duration', 0),
            "meta": {
              **span_message.get('RECORD_ATTRIBUTES', {}),
              **span_message.get('RESOURCE_ATTRIBUTES', {})
            }
          })
        
        for trace_id, spans_list in traces.items():
          spans:List[Span] = []
          for span in spans_list:
            meta = span.get('meta', {})
            # all the meta keys need to be strings
            for key, value in meta.items():
              # dicts and lists become json strings
              if isinstance(value, dict) or isinstance(value, list):
                meta[key] = json.dumps(value) 
              else:
                meta[key] = str(value)
            spans.append(Span(
              service=span.get('service', 'unknown'),
              name=span.get('name', 'unknown'),
              resource=span.get('resource', 'unknown'),
              traceID=trace_id,
              spanID=span.get('span_id', 0),
              parentID=span.get('parent_id', 0),
              start=span.get('date', 0),
              duration=span.get('duration', 0),
              meta=meta,
              error=0,
            ))
          trace_chunk = TraceChunk(
            priority = 1,
            spans = spans
          )
          trace_chunks.append(trace_chunk)
        agent_payload = AgentPayload(tracerPayloads = [
          TracerPayload(chunks=trace_chunks)
        ])
        url = "https://trace.agent.us3.datadoghq.com/api/v0.2/traces"
        headers = {
            "Dd-Api-Key": self.api_key,
            "Content-Type": "application/x-protobuf",
            "X-Datadog-Reported-Languages": "python",
            #"User-Agent": "Datadog Trace Agent/7.59.0",
        }
        agent_payload_serialized = agent_payload.SerializeToString()
        response = self._session.post(url, headers=headers, data=agent_payload_serialized)
        response.raise_for_status()
        self._span_messages=[]
        yield (response.text,)

    def end_partition(self):
      yield from self.do_log_messages_upload()
      yield from self.do_span_messages_upload()