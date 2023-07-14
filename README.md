# **Streaming de Dados - Apache Kafka e Apache Spark**

Com a velocidade com que fontes de dados são gerados no mundo atual, em certas situações pode haver a necessidade da geração de um streaming de dados, que execute uma operação simples, mas que seja utilizado para monitoramento desses dados. Para isso é necessário uma estrutura de ferramentas específicas, que sejam capaz de trabalhar em situações "real time", exigindo escalabilidade, confiabilidade e tolerância a falhas.

Na análise em tempo real, a fonte dos dados envia informações que são anexadas constantemente, ou seja, o comportamento dos dados pode mudar vertiginosamente em poucos minutos, ou segundos.

Além de uma fonte geradora de dados, um streaming de dados de qualidade necessita de uma ferramenta capaz de realizar a coleta, processamento e armazenamento dos dados. Uma das principais ferramentas para esse propósito é o Apache Kafka, que é fundamentalmente um broker, um intermediário que garante que o fornecimento dos dados não sejam perdidos por falhas durante a comunicação. Além disso, ele é capaz de trabalhar em um sistema de clusters, garantindo cópias de segurança (replication factor) contra falhas e perda de dados por conexão. Ela ainda conta com conectores, que permitem a leitura de diversas fontes de dados. Por fim, ele se utiliza do Apache Zookeeper, para sincronização de informações entre diferentes clusters.

Com isso, há a capacidade de se utilizar ferramentas, como o Apache Spark para apresentar, analisar ou treinar modelos com esse fluxo continuo de dados. No projeto aqui presente será apresentado um caso onde é realizada uma execução de query com o SparkSQL, realizada com processamento de lotes, conforme os dados fluem pelo sistema. Os dados enviados também podem ser posteriormente armazenados, garantindo persistência para consultas futuras, mas é algo que não ocorre aqui.


## **Objetivo**

Construir um fluxo de dados para apresentação em tempo real com, com uma medida de agrupamento, utilizando Apache Spark. Para isso será construido um gerador de dados, que simula a geração de transações financeiras em cartão, onde os dados serão enviados ao Apache Kafka e estarão disponíveis para leitura pelo Apache Spark.


## **Pré-requisitos**

Para executar esse projeto será necessário apenas o Docker e docker-compose, podendo ser utilizada ferramentas semelhantes, como podman. Todas as ferramentas utilizadas serão construidas em containers. Serão eles:

* Coordenação Distribuida - Apache Zookeeper
* Broker - Apache Kafka
* Producer - Gerador de dados Python
* Consumer - Apache Spark


### **Apache Zookeeper e Apache Kafka**

Para execução do Apache Kafka, a partir da imagem selecionada é necessário executar, também, o Apache Zookeeper, que será utilizado por ele para sincronizar as informações dos clusters. O Apache Kafka será utilizado como o intermediário entre o Gerador de dados (Producer) e o Apache Spark (Consumer), garantindo que as informações sejam enviadas e recebidas da forma desejada.


### **Producer - Gerador de dados Python**

Arquivo de extensão .py, que pode ser acessado na pasta [producer](./producer). Consiste em um arquivo que gera dados sintéticos, em formato json, e os envia ao Apache Kafka (broker). O arquivo gera um número aleatório de mensagens para ser enviado ao broker, entre 8 à 20 mensagens em períodos de tempo aleatórios, entre 1 e 60 segundos.


## **Consumer - Apache Spark**

Arquivo de extensão .ipynb, que pode pode ser acessado na pasta [consumer](./consumer). Consiste em um jupyter notebook onde está sendo realizada a conexão ao broker, para consumir os dados ali armazenados, gerar uma estrutura a eles e imprimir os dados pela agregação a cada 5 segundos, podendo ter seu tempo alterado.


# **Executando o Projeto**

Para realizar a execução do projeto basta executar via linha de comando, dentro da pasta em que foi realizado o Download e executar o comando `docker-compose up`. O docker irá buscar as imagens para Download e preparar a estrutura dos containers conforme está descrito no arquivo [docker-compose.ymldocker-compose.yml](./docker-compose.yml).

O terminal irá apresentar um link para conexão ao jupyter notebook, onde está o consumer, que basta ser copiado e colado para o navegador. Caso prefira, a visualização das informações pode ser mais clara utilizando uma interface gráfica, como o docker desktop, uma vez que é possível acessar o terminal de cada aplicação por ali, não havendo sobreposição de informações.

Acessando o jupyter notebook, basta acessar a pasta work e executar o jupyter notebook ali presente. Os dados são gerados continuamente, sendo necessário interromper a execução do notebook. Os dados no producer também são gerados continuamente, necessitando a paralização do container.
