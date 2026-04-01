package com.example.bigdata;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import com.espertech.esper.common.client.json.minimaljson.Json;

/**
 * Samowystarczalna demonstracja sterowania czasem Espera przez pole "its".
 *
 * Schemat zdarzenia: KursAkcji (spolka, kursOtwarcia, its)
 *
 * Zapytanie: średnia krocząca kursu otwarcia w oknie 3 dni (win:time(3 days))
 * – czyli zapytanie uzależnione od czasu systemowego Espera.
 *
 * Dane zawierają zdarzenia z różnych dni – Esper nie wie nic o czasie
 * systemowym (wyłączony InternalTimer), sterujemy nim ręcznie przez its.
 *
 * Zależności (Maven):
 *   espertech:esper-common
 *   espertech:esper-compiler
 *   espertech:esper-runtime
 */
public class EsperItsDemo {

    // ---------------------------------------------------------------------------
    // Schemat EPL
    // ---------------------------------------------------------------------------
    private static final String SCHEMA = """
            @public @buseventtype
            create json schema KursAkcji(
                spolka string,
                kursOtwarcia double,
                its long
            );
            """;

    // ---------------------------------------------------------------------------
    // Zapytanie – średnia krocząca w oknie 3 dni sterowanym czasem Espera
    // ---------------------------------------------------------------------------
    private static final String QUERY = """
            @name('avg3days')
            select
                spolka,
                avg(kursOtwarcia) as srednia,
                count(*) as liczba
            from KursAkcji#time(3 days)
            group by spolka
            output snapshot every 1 events;
            """;

    // ---------------------------------------------------------------------------
    // Dane – celowo mają zdarzenia o tym samym its (ta sama chwila giełdowa)
    //        oraz duże przeskoki czasu między sesjami
    // ---------------------------------------------------------------------------
    private static final String[] DATA = {
            // Sesja 1: 2001-09-04  its = 999648000000
            "{\"spolka\":\"Apple\",  \"kursOtwarcia\":18.50, \"its\":999648000000}",
            "{\"spolka\":\"IBM\",    \"kursOtwarcia\":100.15,\"its\":999648000000}",
            "{\"spolka\":\"Intel\",  \"kursOtwarcia\":27.56, \"its\":999648000000}",

            // Sesja 2: 2001-09-05  its = 999734400000  (+1 dzień)
            "{\"spolka\":\"Apple\",  \"kursOtwarcia\":18.24, \"its\":999734400000}",
            "{\"spolka\":\"IBM\",    \"kursOtwarcia\":101.50,\"its\":999734400000}",
            "{\"spolka\":\"Intel\",  \"kursOtwarcia\":26.94, \"its\":999734400000}",

            // Sesja 3: 2001-09-06  its = 999820800000  (+2 dni od sesji 1)
            "{\"spolka\":\"Apple\",  \"kursOtwarcia\":18.40, \"its\":999820800000}",
            "{\"spolka\":\"IBM\",    \"kursOtwarcia\":100.68,\"its\":999820800000}",
            "{\"spolka\":\"Intel\",  \"kursOtwarcia\":26.76, \"its\":999820800000}",

            // Sesja 4: 2001-09-07  its = 999907200000  (+3 dni od sesji 1)
            // Sesja 1 WYPADA z okna 3 dni – średnia powinna się zmienić
            "{\"spolka\":\"Apple\",  \"kursOtwarcia\":17.50, \"its\":999907200000}",
            "{\"spolka\":\"IBM\",    \"kursOtwarcia\":97.90, \"its\":999907200000}",
            "{\"spolka\":\"Intel\",  \"kursOtwarcia\":26.16, \"its\":999907200000}",
    };

    // ---------------------------------------------------------------------------
    // main
    // ---------------------------------------------------------------------------
    public static void main(String[] args) throws EPCompileException, EPDeployException, EPUndeployException {

        // 1. Konfiguracja – wyłączamy timer systemowy
        Configuration config = new Configuration();
        config.getRuntime().getThreading().setInternalTimerEnabled(false);

        // 2. Kompilacja
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        CompilerArguments compilerArgs = new CompilerArguments(config);
        EPCompiled compiled = compiler.compile(SCHEMA + QUERY, compilerArgs);

        // 3. Runtime i deployment
        EPRuntime runtime = EPRuntimeProvider.getRuntime("its-demo", config);
        runtime.getDeploymentService().undeployAll();
        EPDeployment deployment = runtime.getDeploymentService().deploy(compiled);

        // 4. Listener – drukuje wyniki po każdym zdarzeniu
        EPStatement stmt = runtime.getDeploymentService()
                .getStatement(deployment.getDeploymentId(), "avg3days");

        stmt.addListener((newData, oldData, s, rt) -> {
            if (newData == null) return;
            for (EventBean eb : newData) {
                System.out.printf("  %-8s  srednia=%.2f  w oknie=%s zdarzen%n",
                        eb.get("spolka"),
                        ((Number) eb.get("srednia")).doubleValue(),
                        eb.get("liczba"));
            }
        });

        // 5. Wysyłka zdarzeń z kontrolą czasu przez its
        long previousIts = Long.MIN_VALUE;
        for (String json : DATA) {
            long its = Json.parse(json).asObject().getLong("its", previousIts);

            if (its != previousIts) {
                runtime.getEventService().advanceTime(its);
                System.out.printf("%n>>> advanceTime(%d) = %s%n", its, new java.util.Date(its));
                previousIts = its;
            }

            runtime.getEventService().sendEventJson(json, "KursAkcji");
        }

        System.out.println("\nGotowe.");
        runtime.destroy();
    }
}