# trab_G1_INF1304
Trabalho de g1 das alunas Maria Eduarda da Fonseca Gonçalves Santos e Mayara Ramos Damazio para da discplinea de distribuição e concorrência do período de 2026.2

Para fazer o consumidor funcionar:

1 - Compilar o consumer, dentro de consumer-service:

    mvn clean package

2 - Voltar para a raiz do projeto, onde está o docker-compose.yaml:

    cd ..

3 - Subir os 3 brokers + o consumer que quer testar:

    sudo docker-compose up -d kafka-1 kafka-2 kafka-3 consumer-temperatura

Para criar os outros consumers:

    sudo docker-compose up -d consumer-energia

    sudo docker-compose up -d consumer-vibracao

4 - Ver os logs do consumer:

    sudo docker-compose logs -f consumer-temperatura

5 - Em outro terminal, entrar no kafka-1:

    sudo docker-compose exec kafka-1 bash

6 - Dentro do container, iniciar um producer manual para o tópico:

    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka-1:9092 \
    --topic temperatura

7 - Digitar uma mensagem de teste:

    {"sensorID":1,"setor":"setor-a","temperatura":25.4,"timestamp":"2026-09-20T12:00:00Z"}

Aí é só olhar o outro terminal com logs -f para ver o consumer processando.

8 - Para parar tudo depois:

    sudo docker-compose down

Esse é o ciclo básico: compilar → subir → abrir logs → simular producer → observar consumer → derrubar.

Para ver se os containers estão no ar:

    sudo docker ps

Para subir vários (N quanitdades): 

    sudo docker-compose up -d \ 
    --scale consumer-temperatura=N \ 
    kafka-1 kafka-2 kafka-3 consumer-temperatura

Para derrubar:

    Derrubar tudo:

        sudo docker-compose down

    Derrubar serviço específico:

        sudo docker-compose stop onsumer-temperatura
