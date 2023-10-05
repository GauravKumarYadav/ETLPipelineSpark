from dataclasses import dataclass
import os 

@dataclass
class StreamingDataClass1:
    streaming_path: str = os.path.join(os.getcwd(),'Data','Streaming')
