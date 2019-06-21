import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.util.toolbox.AggregationStrategies;

import java.lang.*;
import java.util.ArrayList;

import static org.apache.activemq.camel.component.ActiveMQComponent.activeMQComponent;

public class Main {
    public static void main(String[] args) {
        try {
            RouteBuilder builder = new RouteBuilder() {
                public void configure() {
                    restConfiguration().component("restlet").host("localhost").port(3000);

                    //expondo alguns endpoints REST
                    rest()
                        .post("/pedidos").to("direct:pedidosIn")
                        .post("/cozinha").to("direct:cozinhaIn")
                        .post("/faturamento").to("direct:faturamentoIn")
                        .post("/cozinhaCallback").to("direct:callbackIn")
                        .post("/faturamentoCallback").to("direct:callbackIn")
                        .post("/entrega").to("direct:entregaIn");

                    //postando em uma fila
                    from("direct:pedidosIn").to("activemq:pedidos?exchangePattern=InOnly").transform(constant(null));

                    //esses 2 endpoints poderiam ser chamados através de rotas "direct:"
                    //usando jetty aqui apenas para demonstração
                    from("activemq:pedidos?acknowledgementModeName=CLIENT_ACKNOWLEDGE")
                        .multicast()
                            .parallelProcessing()
                            .to("jetty:http://localhost:3000/cozinha?bridgeEndpoint=true")
                            .to("jetty:http://localhost:3000/faturamento?bridgeEndpoint=true")
                        .end();

                    //retorno das APIs postando em filas
                    from("direct:cozinhaIn").to("activemq:cozinha?exchangePattern=InOnly").transform(constant(null));
                    from("direct:faturamentoIn").to("activemq:faturamento?exchangePattern=InOnly").transform(constant(null));

                    //workers com delay, simulando uma task long running
                    //ao final, elas chamam um endpoint de callback...
                    from("activemq:cozinha?acknowledgementModeName=CLIENT_ACKNOWLEDGE")
                        .delayer(5000)
                        .to("jetty:http://localhost:3000/cozinhaCallback?bridgeEndpoint=true");

                    from("activemq:faturamento?acknowledgementModeName=CLIENT_ACKNOWLEDGE")
                        .delayer(8000)
                        .to("jetty:http://localhost:3000/faturamentoCallback?bridgeEndpoint=true");;

                    //... que posta em uma fila única, pra complicar um pouco ;)
                    from("direct:callbackIn")
                        .to("activemq:resultados?exchangePattern=InOnly")
                        .transform(constant(null));

                    //aqui lemos da fila de callbacks e usamos um aggregator para juntar os eventos correlacionando por id_pedido
                    from("activemq:resultados") //client_ack não funciona depois de passar pelo aggregator (by design, aggregator é stateful)
                        .aggregate(AggregationStrategies
                                    .flexible()
                                    .pick(jsonpath("id_pedido"))
                                    .accumulateInCollection(ArrayList.class))
                            .jsonpath("id_pedido")
                            .completionSize(2) //2 callbacks
                            .completionTimeout(5000L) //janela de tempo para correlacionamento
                        .setHeader("callbacks", simple("${body.size()}"))
                        .setBody(simple("{ \"id_pedido\": ${body[0]}}"))
                        .filter(header("callbacks").isGreaterThan(1))
                        .to("jetty:http://localhost:3000/entrega?bridgeEndpoint=true"); //simulando chamada para endpoint externo

                    //recepção de uma entrega postando para uma fila
                    from("direct:entregaIn").to("activemq:entregas?exchangePattern=InOnly").transform(constant(null));
                }
            };

            CamelContext myCamelContext = new DefaultCamelContext();
            myCamelContext.addComponent("activemq", activeMQComponent("tcp://localhost:61616"));
            myCamelContext.addRoutes(builder);
            myCamelContext.start();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
