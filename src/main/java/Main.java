import java.util.stream.Stream;

import grakn.client.GraknClient;
import grakn.core.concept.answer.ConceptMap;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlInsert;

import static graql.lang.Graql.and;
import static graql.lang.Graql.type;
import static graql.lang.Graql.var;

public class Main {
    public static void main(String[] args) {
        GraknClient client = new GraknClient("localhost:48555");
        client.keyspaces().delete("social_network");

        GraknClient.Session session = client.session("social_network");

        /*
         * Ontología
         */
        GraknClient.Transaction writeTransaction = session.transaction().write();
        GraqlDefine ontology = Graql.define(
                type("person").sub("entity").plays("targetPerson").has("name"),
                type("name").sub("attribute").datatype("string"),
                type("targetPerson").sub("role"),
                type("parent").sub("relation").relates("targetPerson").relates("targetPerson"),
                type("spouse").sub("relation").relates("targetPerson").relates("targetPerson"),
                type("brother").sub("relation").relates("targetPerson").relates("targetPerson"),
                type("cousin").sub("relation").relates("targetPerson").relates("targetPerson")
        );
        writeTransaction.execute(ontology);
        writeTransaction.commit();
        System.out.println("Ontología definida.");

        /*
         * Reglas
         */
        GraqlDefine rules = Graql.define(
                type("brotherhoodRule").sub("rule").when(
                        and(
                                var().rel("targetPerson", "f").rel("c1").isa("parent"),
                                var().rel("targetPerson", "f").rel("c2").isa("parent"),
                                var().rel("targetPerson", "m").rel("c1").isa("parent"),
                                var().rel("targetPerson", "m").rel("c2").isa("parent"),
                                var().rel("targetPerson", "f").rel("m").isa("spouse")
                        )
                ).then(var().isa("brother").rel("c1").rel("c2")),
                type("cousin1Rule").sub("rule").when(
                        and(
                                var().rel("targetPerson", "p1").rel("m1").isa("spouse"),
                                var().rel("targetPerson", "p1").rel("x").isa("parent"),
                                var().rel("targetPerson", "m1").rel("x").isa("parent"),
                                var().rel("targetPerson", "p2").rel("m2").isa("spouse"),
                                var().rel("targetPerson", "p2").rel("y").isa("parent"),
                                var().rel("targetPerson", "m2").rel("y").isa("parent"),
                                var().rel("targetPerson", "p1").rel("p2").isa("brother")
                        )
                ).then(var().isa("cousin").rel("x").rel("y")),
                type("cousin2Rule").sub("rule").when(
                        and(
                                var().rel("targetPerson", "p1").rel("m1").isa("spouse"),
                                var().rel("targetPerson", "p1").rel("x").isa("parent"),
                                var().rel("targetPerson", "m1").rel("x").isa("parent"),
                                var().rel("targetPerson", "p2").rel("m2").isa("spouse"),
                                var().rel("targetPerson", "p2").rel("y").isa("parent"),
                                var().rel("targetPerson", "m2").rel("y").isa("parent"),
                                var().rel("targetPerson", "m1").rel("m2").isa("brother")
                        )
                ).then(var().isa("cousin").rel("x").rel("y")),
                type("cousin3Rule").sub("rule").when(
                        and(
                                var().rel("targetPerson", "p1").rel("m1").isa("spouse"),
                                var().rel("targetPerson", "p1").rel("x").isa("parent"),
                                var().rel("targetPerson", "m1").rel("x").isa("parent"),
                                var().rel("targetPerson", "p2").rel("m2").isa("spouse"),
                                var().rel("targetPerson", "p2").rel("y").isa("parent"),
                                var().rel("targetPerson", "m2").rel("y").isa("parent"),
                                var().rel("targetPerson", "p1").rel("m2").isa("brother")
                        )
                ).then(var().isa("cousin").rel("x").rel("y"))
        );
        writeTransaction = session.transaction().write();
        writeTransaction.execute(rules);
        writeTransaction.commit();
        System.out.println("Reglas definidas.");

        /*
         * Datos
         */
        GraqlInsert insertQuery = Graql.insert(
                var("roberto").isa("person").has("name", "Roberto"),
                var("celeste").isa("person").has("name", "Celeste"),
                var("pepe").isa("person").has("name", "Pepe"),
                var("daniela").isa("person").has("name", "Daniela"),
                var("hernan").isa("person").has("name", "Hernan"),
                var("maria").isa("person").has("name", "Maria"),
                var("santino").isa("person").has("name", "Santino"),
                var("belen").isa("person").has("name", "Belen"),

                var().rel("targetPerson", "roberto").rel("targetPerson", "celeste").isa("spouse"),
                var().rel("targetPerson", "roberto").rel("targetPerson", "daniela").isa("parent"),
                var().rel("targetPerson", "celeste").rel("targetPerson", "daniela").isa("parent"),
                var().rel("targetPerson", "roberto").rel("targetPerson", "hernan").isa("parent"),
                var().rel("targetPerson", "celeste").rel("targetPerson", "hernan").isa("parent"),

                var().rel("targetPerson", "pepe").rel("targetPerson", "daniela").isa("spouse"),
                var().rel("targetPerson", "pepe").rel("targetPerson", "santino").isa("parent"),
                var().rel("targetPerson", "daniela").rel("targetPerson", "santino").isa("parent"),

                var().rel("targetPerson", "hernan").rel("targetPerson", "maria").isa("spouse"),
                var().rel("targetPerson", "hernan").rel("targetPerson", "belen").isa("parent"),
                var().rel("targetPerson", "maria").rel("targetPerson", "belen").isa("parent")
        );
        writeTransaction = session.transaction().write();
        writeTransaction.execute(insertQuery);
        writeTransaction.commit();
        System.out.println("Datos insertados.");

        // Leyendo persona
        GraknClient.Transaction readTransaction = session.transaction().read();
        GraqlGet getQuery = Graql.match(
                var("c").isa("cousin").rel("p1").rel("p2"),
                var("p1").has("name", var("p1_name")),
                var("p2").has("name", var("p2_name")),
                var("p1_name").neq(var("p2_name"))
        ).get("p1_name", "p2_name");
        Stream<ConceptMap> answers = readTransaction.stream(getQuery);
        answers.forEach(answer -> System.out.println(answer.get("p1_name").asAttribute().value() + " es primo/a de " +  answer.get("p2_name").asAttribute().value()));
        readTransaction.close();

        session.close();
        client.close();
    }
}
