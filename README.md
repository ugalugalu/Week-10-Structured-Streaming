# Week-10-Structured-Streaming

This is a project to analyse network traffic and detect network anomalies.

Data was produced into a locally installed Kafka instance on network-traffic topic. Below is a screen shot of kafka running instance on docker

![image](https://github.com/ugalugalu/Week-10-Structured-Streaming/assets/54645939/955c12fe-bc1c-44a8-861d-9e439e421e0a)

Data was ingested into spark for Structured analysis and later produced into another topic running in Kafla network-traffic-producer.

Below is a screen shot of the two topics.

![image](https://github.com/ugalugalu/Week-10-Structured-Streaming/assets/54645939/b6d71c38-e0bb-4d56-bfa7-0a1be02755ed)

Another script has been included for visualisation on streamlit(Structured_with_streamlit.py)

