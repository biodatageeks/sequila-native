window.BENCHMARK_DATA = {
  "lastUpdate": 1731784497662,
  "repoUrl": "https://github.com/biodatageeks/sequila-native",
  "entries": {
    "Criterion.rs Benchmark": [
      {
        "commit": {
          "author": {
            "name": "biodatageeks",
            "username": "biodatageeks"
          },
          "committer": {
            "name": "biodatageeks",
            "username": "biodatageeks"
          },
          "id": "373014bcf88d4ec6dac579345d8a32912df86cc4",
          "message": "Draft benchmark",
          "timestamp": "2024-11-16T18:38:40Z",
          "url": "https://github.com/biodatageeks/sequila-native/pull/39/commits/373014bcf88d4ec6dac579345d8a32912df86cc4"
        },
        "date": 1731784495823,
        "tool": "cargo",
        "benches": [
          {
            "name": "Coitrees-fBrain-DS14718-exons-1-2",
            "value": 85456662,
            "range": "± 2018281",
            "unit": "ns/iter"
          },
          {
            "name": "IntervalTree-fBrain-DS14718-exons-1-2",
            "value": 184925780,
            "range": "± 557564",
            "unit": "ns/iter"
          },
          {
            "name": "ArrayIntervalTree-fBrain-DS14718-exons-1-2",
            "value": 146976520,
            "range": "± 8612028",
            "unit": "ns/iter"
          },
          {
            "name": "AIList-fBrain-DS14718-exons-1-2",
            "value": 169223763,
            "range": "± 1683638",
            "unit": "ns/iter"
          },
          {
            "name": "Coitrees-exons-fBrain-DS14718-2-1",
            "value": 87032721,
            "range": "± 3146279",
            "unit": "ns/iter"
          },
          {
            "name": "IntervalTree-exons-fBrain-DS14718-2-1",
            "value": 190205810,
            "range": "± 12832081",
            "unit": "ns/iter"
          },
          {
            "name": "ArrayIntervalTree-exons-fBrain-DS14718-2-1",
            "value": 131516724,
            "range": "± 2318158",
            "unit": "ns/iter"
          },
          {
            "name": "AIList-exons-fBrain-DS14718-2-1",
            "value": 178991973,
            "range": "± 951849",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}