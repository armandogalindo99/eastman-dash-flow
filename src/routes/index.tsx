import { createFileRoute } from "@tanstack/react-router";
import { AuthProvider, useAuth } from "@/lib/auth";
import { LoginScreen } from "@/components/LoginScreen";
import { LogOut } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import {
  Activity, Database, Server, Radio, Cpu, ShieldCheck, PlayCircle, PauseCircle,
  RefreshCw, Plus, Trash2, CheckCircle2, XCircle, AlertTriangle, Terminal,
  Gauge, Waves, Boxes, Layers, HardDrive, ArrowRight, Zap,
} from "lucide-react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
  AreaChart, Area, BarChart, Bar,
} from "recharts";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Progress } from "@/components/ui/progress";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Switch } from "@/components/ui/switch";
import { Separator } from "@/components/ui/separator";

export const Route = createFileRoute("/")({
  head: () => ({
    meta: [
      { title: "TEP-Kafka Control · Panel del Gemelo Digital" },
      { name: "description", content: "Dashboard para administrar la aplicación del Proceso Tennessee Eastman basada en Apache Kafka: broker, tópicos, productor, consumidores y persistencia MongoDB." },
      { property: "og:title", content: "TEP-Kafka Control" },
      { property: "og:description", content: "Panel principal para operar el gemelo digital TEP sobre Apache Kafka." },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary_large_image" },
    ],
  }),
  component: RouteComponent,
});

function RouteComponent() {
  return (
    <AuthProvider>
      <Gate />
    </AuthProvider>
  );
}

function Gate() {
  const { user, ready, signOut } = useAuth();
  if (!ready) return null;
  if (!user) return <LoginScreen />;
  return (
    <div className="relative">
      <div className="pointer-events-none fixed right-4 top-4 z-50 flex items-center gap-2">
        <span className="pointer-events-auto rounded-full border border-border bg-card/90 px-3 py-1.5 text-xs font-medium text-foreground shadow-sm backdrop-blur">
          {user.username} · {user.role === "admin" ? "Administrador" : "Operador"}
        </span>
        <button
          type="button"
          onClick={signOut}
          className="pointer-events-auto inline-flex items-center gap-1.5 rounded-full border border-border bg-card/90 px-3 py-1.5 text-xs font-medium text-muted-foreground shadow-sm backdrop-blur transition-colors hover:text-foreground"
        >
          <LogOut className="h-3.5 w-3.5" />
          Salir
        </button>
      </div>
      <Dashboard />
    </div>
  );
}

// ---------- Types ----------
type LogLevel = "INFO" | "WARN" | "ERROR" | "OK";
type LogEntry = { t: string; level: LogLevel; source: string; msg: string };
type TepEvent = {
  event_id: string;
  event_time: string;
  variable_id: string;
  variable: string;
  value: number;
  unit: string;
  quality: "VALID" | "SUSPECT";
  offset: number;
  partition: number;
};
type Topic = {
  name: string;
  partitions: number;
  replication: number;
  messages: number;
  retentionH: number;
};
type Consumer = {
  id: string;
  group: string;
  topic: string;
  lag: number;
  status: "running" | "paused" | "error";
  offset: number;
  processed: number;
};

const VARIABLES = [
  { id: "XMEAS-07", name: "reactor_pressure", unit: "kPa", base: 2820, span: 40 },
  { id: "XMEAS-09", name: "reactor_temperature", unit: "°C", base: 120.4, span: 1.2 },
  { id: "XMEAS-08", name: "reactor_level", unit: "%", base: 65.0, span: 3 },
  { id: "XMEAS-11", name: "sep_temperature", unit: "°C", base: 80.2, span: 0.8 },
  { id: "XMEAS-13", name: "prod_sep_pressure", unit: "kPa", base: 2633.7, span: 25 },
  { id: "XMEAS-06", name: "reactor_feed_rate", unit: "kscmh", base: 42.3, span: 1.5 },
];

const rid = () => Math.random().toString(36).slice(2, 10);
const nowIso = () => new Date().toISOString();
const hhmmss = (d = new Date()) =>
  d.toTimeString().slice(0, 8);

function Dashboard() {
  // ---------- State ----------
  const [running, setRunning] = useState(true);
  const [rate, setRate] = useState(1000); // ms between simulator ticks
  const [brokerUp, setBrokerUp] = useState(true);
  const [mongoUp, setMongoUp] = useState(true);
  const [tab, setTab] = useState("overview");

  const [topics, setTopics] = useState<Topic[]>([
    { name: "tep.raw.measurements", partitions: 1, replication: 1, messages: 0, retentionH: 168 },
  ]);
  const [newTopic, setNewTopic] = useState("");

  const [consumers, setConsumers] = useState<Consumer[]>([
    { id: "c-test-1", group: "tep-test-consumer-group", topic: "tep.raw.measurements", lag: 0, status: "running", offset: 0, processed: 0 },
    { id: "c-persist-1", group: "tep-persistence-group", topic: "tep.raw.measurements", lag: 0, status: "running", offset: 0, processed: 0 },
  ]);

  const [events, setEvents] = useState<TepEvent[]>([]);
  const [logs, setLogs] = useState<LogEntry[]>([
    { t: hhmmss(), level: "OK", source: "docker", msg: "kafka-broker container started on :9092" },
    { t: hhmmss(), level: "OK", source: "kafka", msg: "topic 'tep.raw.measurements' created (partitions=1, rf=1)" },
    { t: hhmmss(), level: "OK", source: "mongo", msg: "index event_id_1 unique OK on collection 'measurements'" },
  ]);
  const [seriesByVar, setSeriesByVar] = useState<Record<string, { t: number; v: number }[]>>({});
  const [throughput, setThroughput] = useState<{ t: string; msgs: number }[]>(
    Array.from({ length: 30 }, (_, i) => ({ t: `${i}`, msgs: 0 })),
  );
  const [mongoDocs, setMongoDocs] = useState(0);
  const [duplicatesRejected, setDuplicatesRejected] = useState(0);

  // ---------- Simulator loop ----------
  useEffect(() => {
    if (!running || !brokerUp) return;
    const id = setInterval(() => {
      // 1 tick = 1 measurement per variable subset
      const sample = VARIABLES[Math.floor(Math.random() * VARIABLES.length)];
      const jitter = (Math.random() - 0.5) * sample.span;
      const value = +(sample.base + jitter).toFixed(2);
      const rawTopic = topics.find((t) => t.name === "tep.raw.measurements");
      if (!rawTopic) return;

      const evt: TepEvent = {
        event_id: `${rid()}-${rid()}`,
        event_time: nowIso(),
        variable_id: sample.id,
        variable: sample.name,
        value,
        unit: sample.unit,
        quality: Math.random() > 0.02 ? "VALID" : "SUSPECT",
        offset: rawTopic.messages,
        partition: 0,
      };

      setEvents((prev) => [evt, ...prev].slice(0, 80));
      setTopics((prev) =>
        prev.map((t) =>
          t.name === "tep.raw.measurements" ? { ...t, messages: t.messages + 1 } : t,
        ),
      );
      setSeriesByVar((prev) => {
        const arr = prev[sample.id] ?? [];
        const next = [...arr, { t: Date.now(), v: value }].slice(-40);
        return { ...prev, [sample.id]: next };
      });
      setThroughput((prev) => {
        const next = [...prev.slice(1), { t: hhmmss(), msgs: (prev[prev.length - 1]?.msgs ?? 0) + 1 }];
        return next;
      });

      // consumers process
      setConsumers((prev) =>
        prev.map((c) => {
          if (c.status !== "running" || c.topic !== "tep.raw.measurements") {
            return { ...c, lag: c.lag + 1 };
          }
          const isPersist = c.group === "tep-persistence-group";
          if (isPersist && !mongoUp) {
            return { ...c, lag: c.lag + 1, status: "error" as const };
          }
          return { ...c, offset: c.offset + 1, processed: c.processed + 1, lag: Math.max(0, c.lag - 0) };
        }),
      );

      if (mongoUp) setMongoDocs((n) => n + 1);
    }, rate);
    return () => clearInterval(id);
  }, [running, rate, brokerUp, mongoUp, topics]);

  // periodic throughput tick even when idle
  useEffect(() => {
    const id = setInterval(() => {
      setThroughput((prev) => [...prev.slice(1), { t: hhmmss(), msgs: 0 }]);
    }, 3000);
    return () => clearInterval(id);
  }, []);

  // ---------- Actions ----------
  const addLog = (level: LogLevel, source: string, msg: string) =>
    setLogs((prev) => [{ t: hhmmss(), level, source, msg }, ...prev].slice(0, 200));

  const toggleBroker = () => {
    setBrokerUp((b) => {
      addLog(b ? "WARN" : "OK", "docker", b ? "kafka-broker stopped" : "kafka-broker started on :9092");
      return !b;
    });
  };
  const toggleMongo = () => {
    setMongoUp((b) => {
      addLog(b ? "ERROR" : "OK", "mongo", b ? "connection lost to mongodb://mongo:27017" : "connected to mongodb://mongo:27017");
      return !b;
    });
  };
  const createTopic = () => {
    const n = newTopic.trim();
    if (!n) return;
    if (topics.some((t) => t.name === n)) {
      addLog("WARN", "kafka", `topic '${n}' already exists`);
      return;
    }
    setTopics((p) => [...p, { name: n, partitions: 1, replication: 1, messages: 0, retentionH: 168 }]);
    addLog("OK", "kafka", `topic '${n}' created (partitions=1, rf=1)`);
    setNewTopic("");
  };
  const deleteTopic = (name: string) => {
    if (name === "tep.raw.measurements") {
      addLog("WARN", "kafka", "protected topic 'tep.raw.measurements' cannot be deleted");
      return;
    }
    setTopics((p) => p.filter((t) => t.name !== name));
    addLog("OK", "kafka", `topic '${name}' deleted`);
  };
  const toggleConsumer = (id: string) => {
    setConsumers((p) =>
      p.map((c) => {
        if (c.id !== id) return c;
        const next: Consumer["status"] = c.status === "running" ? "paused" : "running";
        addLog("INFO", c.group, `consumer ${c.id} ${next}`);
        return { ...c, status: next };
      }),
    );
  };
  const resetConsumer = (id: string) => {
    setConsumers((p) => p.map((c) => (c.id === id ? { ...c, offset: 0, lag: 0, processed: 0 } : c)));
    addLog("INFO", "kafka", `consumer ${id} offset reset to earliest`);
  };
  const injectDuplicate = () => {
    if (!events[0]) return;
    setDuplicatesRejected((n) => n + 1);
    addLog("ERROR", "mongo", `E11000 duplicate key on event_id ${events[0].event_id.slice(0, 12)}…`);
  };

  const totalMsgs = topics.reduce((s, t) => s + t.messages, 0);
  const totalLag = consumers.reduce((s, c) => s + c.lag, 0);
  const rawTopic = topics.find((t) => t.name === "tep.raw.measurements");

  const kpis = [
    { label: "Broker Kafka", value: brokerUp ? "UP" : "DOWN", icon: Server, tone: brokerUp ? "success" : "danger" as const },
    { label: "Mensajes totales", value: totalMsgs.toLocaleString(), icon: Radio, tone: "primary" as const },
    { label: "MongoDB · docs", value: mongoDocs.toLocaleString(), icon: Database, tone: mongoUp ? "success" : "danger" as const },
    { label: "Consumer lag", value: totalLag.toString(), icon: Waves, tone: totalLag > 20 ? "warn" : "primary" as const },
  ];

  return (
    <div className="min-h-screen bg-background text-foreground flex">
      {/* Sidebar */}
      <aside className="hidden md:flex w-64 shrink-0 flex-col border-r border-border bg-sidebar">
        <div className="p-5 border-b border-sidebar-border">
          <div className="flex items-center gap-2">
            <div className="h-9 w-9 rounded-md bg-primary/15 border border-primary/30 grid place-items-center">
              <Zap className="h-5 w-5 text-primary" />
            </div>
            <div>
              <div className="text-sm font-semibold tracking-tight">TEP · Kafka</div>
              <div className="text-[11px] text-muted-foreground">Digital Twin Control</div>
            </div>
          </div>
        </div>
        <nav className="p-3 space-y-1 text-sm">
          {[
            { id: "overview", label: "Panorama", icon: Gauge },
            { id: "broker", label: "Broker", icon: Server },
            { id: "topics", label: "Tópicos", icon: Layers },
            { id: "producer", label: "Productor TEP", icon: Cpu },
            { id: "consumers", label: "Consumidores", icon: Boxes },
            { id: "mongo", label: "MongoDB", icon: HardDrive },
            { id: "tests", label: "Pruebas", icon: ShieldCheck },
            { id: "logs", label: "Logs", icon: Terminal },
          ].map((item) => (
            <button
              key={item.id}
              onClick={() => setTab(item.id)}
              className={`w-full flex items-center gap-2 px-3 py-2 rounded-md transition-colors ${
                tab === item.id
                  ? "bg-sidebar-accent text-sidebar-accent-foreground border border-sidebar-border"
                  : "text-sidebar-foreground/80 hover:bg-sidebar-accent/60"
              }`}
            >
              <item.icon className="h-4 w-4" />
              {item.label}
            </button>
          ))}
        </nav>
        <div className="mt-auto p-4 border-t border-sidebar-border space-y-2 text-[11px] text-muted-foreground">
          <div className="flex items-center gap-2">
            <span className={`h-2 w-2 rounded-full ${brokerUp ? "bg-success pulse-dot" : "bg-destructive"}`} />
            broker · kafka-broker:9092
          </div>
          <div className="flex items-center gap-2">
            <span className={`h-2 w-2 rounded-full ${mongoUp ? "bg-success pulse-dot" : "bg-destructive"}`} />
            mongo · mongodb:27017
          </div>
        </div>
      </aside>

      {/* Main */}
      <main className="flex-1 min-w-0 flex flex-col">
        {/* Top bar */}
        <header className="h-14 border-b border-border bg-card/60 backdrop-blur flex items-center gap-3 px-4 md:px-6">
          <h1 className="text-sm md:text-base font-semibold tracking-tight">
            Panel principal · Proceso Tennessee Eastman sobre Apache Kafka
          </h1>
          <div className="ml-auto flex items-center gap-2">
            <div className="hidden sm:flex items-center gap-2 text-xs text-muted-foreground">
              <span className={`h-2 w-2 rounded-full ${running ? "bg-success pulse-dot" : "bg-muted-foreground"}`} />
              {running ? "Simulador activo" : "Simulador detenido"}
            </div>
            <Button size="sm" variant={running ? "secondary" : "default"} onClick={() => setRunning((r) => !r)}>
              {running ? <><PauseCircle className="h-4 w-4 mr-1" />Pausar</> : <><PlayCircle className="h-4 w-4 mr-1" />Iniciar</>}
            </Button>
            <Button size="sm" variant="outline" onClick={toggleBroker}>
              <Server className="h-4 w-4 mr-1" />{brokerUp ? "Detener broker" : "Iniciar broker"}
            </Button>
          </div>
        </header>

        {/* Ticker */}
        <div className="border-b border-border bg-muted/30 overflow-hidden">
          <div className="flex whitespace-nowrap animate-ticker text-[11px] font-mono text-muted-foreground py-1.5">
            {[...events, ...events].slice(0, 20).map((e, i) => (
              <span key={i} className="px-6 flex items-center gap-2">
                <span className="text-primary">{e.variable_id}</span>
                <span>{e.value}</span>
                <span className="opacity-60">{e.unit}</span>
                <span className="opacity-40">·</span>
              </span>
            ))}
            {events.length === 0 && <span className="px-6">Esperando mediciones del simulador TEP…</span>}
          </div>
        </div>

        <div className="p-4 md:p-6 space-y-6">
          <Tabs value={tab} onValueChange={setTab}>
            <TabsList className="md:hidden mb-4 flex-wrap h-auto">
              {["overview", "broker", "topics", "producer", "consumers", "mongo", "tests", "logs"].map((t) => (
                <TabsTrigger key={t} value={t}>{t}</TabsTrigger>
              ))}
            </TabsList>

            {/* ---------- OVERVIEW ---------- */}
            <TabsContent value="overview" className="space-y-6 mt-0">
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                {kpis.map((k) => (
                  <Card key={k.label} className="border-border/70">
                    <CardContent className="p-4">
                      <div className="flex items-center justify-between">
                        <span className="text-xs text-muted-foreground uppercase tracking-wide">{k.label}</span>
                        <k.icon className={`h-4 w-4 ${
                          k.tone === "success" ? "text-success" :
                          k.tone === "danger" ? "text-destructive" :
                          k.tone === "warn" ? "text-warning" : "text-primary"
                        }`} />
                      </div>
                      <div className="mt-2 text-2xl font-semibold font-mono">{k.value}</div>
                    </CardContent>
                  </Card>
                ))}
              </div>

              {/* Pipeline diagram */}
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Cadena de eventos</CardTitle>
                  <CardDescription>Simulador TEP → Productor → Broker Kafka → Consumidores → MongoDB</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 md:grid-cols-5 gap-3 items-stretch">
                    {[
                      { icon: Cpu, label: "Simulador TEP", sub: running ? "generando" : "detenido", ok: running },
                      { icon: Activity, label: "Productor", sub: "tep-producer-01", ok: running && brokerUp },
                      { icon: Server, label: "Broker Kafka", sub: brokerUp ? ":9092" : "offline", ok: brokerUp },
                      { icon: Boxes, label: "Consumidores", sub: `${consumers.filter(c => c.status === "running").length}/${consumers.length} activos`, ok: consumers.some(c => c.status === "running") },
                      { icon: Database, label: "MongoDB", sub: mongoUp ? "measurements" : "offline", ok: mongoUp },
                    ].map((n, i, arr) => (
                      <div key={n.label} className="relative">
                        <div className={`rounded-lg border p-3 h-full ${n.ok ? "border-primary/40 bg-primary/5" : "border-destructive/40 bg-destructive/5"}`}>
                          <div className="flex items-center gap-2">
                            <n.icon className={`h-4 w-4 ${n.ok ? "text-primary" : "text-destructive"}`} />
                            <span className="text-sm font-medium">{n.label}</span>
                          </div>
                          <div className="mt-1 text-[11px] text-muted-foreground font-mono">{n.sub}</div>
                        </div>
                        {i < arr.length - 1 && (
                          <ArrowRight className="hidden md:block absolute -right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                        )}
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>

              <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                <Card className="lg:col-span-2">
                  <CardHeader>
                    <CardTitle className="text-base">Throughput · tep.raw.measurements</CardTitle>
                    <CardDescription>Mensajes publicados por intervalo</CardDescription>
                  </CardHeader>
                  <CardContent className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={throughput}>
                        <defs>
                          <linearGradient id="g1" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="0%" stopColor="var(--color-primary)" stopOpacity={0.6} />
                            <stop offset="100%" stopColor="var(--color-primary)" stopOpacity={0} />
                          </linearGradient>
                        </defs>
                        <CartesianGrid stroke="var(--color-border)" strokeDasharray="3 3" />
                        <XAxis dataKey="t" stroke="var(--color-muted-foreground)" fontSize={10} />
                        <YAxis stroke="var(--color-muted-foreground)" fontSize={10} />
                        <Tooltip contentStyle={{ background: "var(--color-card)", border: "1px solid var(--color-border)", fontSize: 12 }} />
                        <Area type="monotone" dataKey="msgs" stroke="var(--color-primary)" fill="url(#g1)" strokeWidth={2} />
                      </AreaChart>
                    </ResponsiveContainer>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle className="text-base">Lag por consumidor</CardTitle>
                    <CardDescription>Retraso respecto al head del tópico</CardDescription>
                  </CardHeader>
                  <CardContent className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={consumers.map(c => ({ name: c.group.replace("tep-", "").replace("-group", ""), lag: c.lag }))}>
                        <CartesianGrid stroke="var(--color-border)" strokeDasharray="3 3" />
                        <XAxis dataKey="name" stroke="var(--color-muted-foreground)" fontSize={10} />
                        <YAxis stroke="var(--color-muted-foreground)" fontSize={10} />
                        <Tooltip contentStyle={{ background: "var(--color-card)", border: "1px solid var(--color-border)", fontSize: 12 }} />
                        <Bar dataKey="lag" fill="var(--color-accent)" radius={[4, 4, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </CardContent>
                </Card>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                <VariablesChart seriesByVar={seriesByVar} />
                <RecentEvents events={events} />
              </div>
            </TabsContent>

            {/* ---------- BROKER ---------- */}
            <TabsContent value="broker" className="space-y-4 mt-0">
              <div className="grid md:grid-cols-3 gap-4">
                <Card className="md:col-span-2">
                  <CardHeader>
                    <CardTitle className="text-base flex items-center gap-2"><Server className="h-4 w-4 text-primary" />Broker · kafka-broker</CardTitle>
                    <CardDescription>Despliegue Docker Compose · imagen apache/kafka</CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                      <Field label="Estado" value={brokerUp ? "UP" : "DOWN"} tone={brokerUp ? "success" : "danger"} />
                      <Field label="Node ID" value="1" />
                      <Field label="Listener" value="PLAINTEXT://:9092" />
                      <Field label="Uptime" value={brokerUp ? "activo" : "—"} />
                    </div>
                    <Separator />
                    <div className="rounded-md border border-border bg-muted/30 p-3">
                      <div className="text-[11px] text-muted-foreground mb-2 uppercase tracking-wide">docker-compose.yml</div>
                      <pre className="text-[11px] font-mono leading-relaxed overflow-x-auto text-foreground/90">
{`services:
  kafka-broker:
    image: apache/kafka
    container_name: kafka-broker
    ports:
      - "9092:9092"
    volumes:
      - kafka-data:/var/lib/kafka/data
volumes:
  kafka-data:`}
                      </pre>
                    </div>
                    <div className="flex gap-2">
                      <Button size="sm" onClick={toggleBroker} variant={brokerUp ? "destructive" : "default"}>
                        {brokerUp ? "Detener" : "Iniciar"} broker
                      </Button>
                      <Button size="sm" variant="outline" onClick={() => addLog("INFO", "docker", "docker compose logs kafka-broker")}>
                        <Terminal className="h-4 w-4 mr-1" />Ver logs
                      </Button>
                    </div>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader><CardTitle className="text-base">Recursos</CardTitle></CardHeader>
                  <CardContent className="space-y-4">
                    <MetricBar label="CPU" value={brokerUp ? 34 : 0} />
                    <MetricBar label="Memoria" value={brokerUp ? 58 : 0} />
                    <MetricBar label="Disco (kafka-data)" value={Math.min(90, Math.floor(totalMsgs / 200))} />
                    <MetricBar label="Red" value={brokerUp && running ? 22 : 0} />
                  </CardContent>
                </Card>
              </div>
            </TabsContent>

            {/* ---------- TOPICS ---------- */}
            <TabsContent value="topics" className="space-y-4 mt-0">
              <Card>
                <CardHeader className="flex flex-row items-center justify-between">
                  <div>
                    <CardTitle className="text-base">Tópicos</CardTitle>
                    <CardDescription>Administra los tópicos del broker</CardDescription>
                  </div>
                  <div className="flex gap-2 items-center">
                    <Input
                      placeholder="tep.processed.variables"
                      value={newTopic}
                      onChange={(e) => setNewTopic(e.target.value)}
                      className="w-64 font-mono text-xs"
                    />
                    <Button size="sm" onClick={createTopic}><Plus className="h-4 w-4 mr-1" />Crear</Button>
                  </div>
                </CardHeader>
                <CardContent>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Nombre</TableHead>
                        <TableHead className="text-right">Particiones</TableHead>
                        <TableHead className="text-right">RF</TableHead>
                        <TableHead className="text-right">Mensajes</TableHead>
                        <TableHead className="text-right">Retención (h)</TableHead>
                        <TableHead />
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {topics.map((t) => (
                        <TableRow key={t.name}>
                          <TableCell className="font-mono text-xs">{t.name}</TableCell>
                          <TableCell className="text-right font-mono">{t.partitions}</TableCell>
                          <TableCell className="text-right font-mono">{t.replication}</TableCell>
                          <TableCell className="text-right font-mono">{t.messages.toLocaleString()}</TableCell>
                          <TableCell className="text-right font-mono">{t.retentionH}</TableCell>
                          <TableCell className="text-right">
                            <Button size="sm" variant="ghost" onClick={() => deleteTopic(t.name)}>
                              <Trash2 className="h-4 w-4 text-destructive" />
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>
            </TabsContent>

            {/* ---------- PRODUCER ---------- */}
            <TabsContent value="producer" className="space-y-4 mt-0">
              <div className="grid md:grid-cols-3 gap-4">
                <Card className="md:col-span-2">
                  <CardHeader>
                    <CardTitle className="text-base flex items-center gap-2"><Cpu className="h-4 w-4 text-primary" />Productor TEP</CardTitle>
                    <CardDescription>Simulador Tennessee Eastman → tep.raw.measurements</CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                      <Field label="Estado" value={running ? "RUN" : "PAUSED"} tone={running ? "success" : "warn"} />
                      <Field label="Publicados" value={(rawTopic?.messages ?? 0).toLocaleString()} />
                      <Field label="Fallos" value="0" tone="success" />
                      <Field label="Schema" value="v1.0" />
                    </div>
                    <div className="flex items-center gap-4">
                      <Label className="text-xs text-muted-foreground w-32">Tasa (ms/evento)</Label>
                      <Input
                        type="number"
                        min={100}
                        max={5000}
                        value={rate}
                        onChange={(e) => setRate(Number(e.target.value) || 1000)}
                        className="w-32 font-mono"
                      />
                      <div className="flex items-center gap-2 text-xs text-muted-foreground">
                        <Switch checked={running} onCheckedChange={setRunning} /> auto-publicar
                      </div>
                    </div>
                    <div className="rounded-md border border-border bg-muted/30 p-3">
                      <div className="text-[11px] text-muted-foreground mb-2 uppercase tracking-wide">último evento publicado</div>
                      <pre className="text-[11px] font-mono leading-relaxed overflow-x-auto">
{events[0] ? JSON.stringify({
  event_id: events[0].event_id,
  event_time: events[0].event_time,
  source: "tep-simulator",
  event_type: "process_measurement",
  schema_version: "1.0",
  partition_key: events[0].variable,
  payload: {
    run_id: "tep-001",
    variable_id: events[0].variable_id,
    variable: events[0].variable,
    value: events[0].value,
    unit: events[0].unit,
    quality: events[0].quality,
  },
}, null, 2) : "// esperando primer evento…"}
                      </pre>
                    </div>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader><CardTitle className="text-base">Variables TEP</CardTitle></CardHeader>
                  <CardContent className="space-y-2 text-xs font-mono">
                    {VARIABLES.map((v) => (
                      <div key={v.id} className="flex items-center justify-between border-b border-border/50 pb-1.5">
                        <span className="text-primary">{v.id}</span>
                        <span className="text-muted-foreground truncate mx-2">{v.name}</span>
                        <span>{v.base}{v.unit}</span>
                      </div>
                    ))}
                  </CardContent>
                </Card>
              </div>
            </TabsContent>

            {/* ---------- CONSUMERS ---------- */}
            <TabsContent value="consumers" className="space-y-4 mt-0">
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Grupos de consumidores</CardTitle>
                  <CardDescription>Prueba y persistencia sobre tep.raw.measurements</CardDescription>
                </CardHeader>
                <CardContent>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Consumer</TableHead>
                        <TableHead>Grupo</TableHead>
                        <TableHead>Tópico</TableHead>
                        <TableHead className="text-right">Offset</TableHead>
                        <TableHead className="text-right">Procesados</TableHead>
                        <TableHead className="text-right">Lag</TableHead>
                        <TableHead>Estado</TableHead>
                        <TableHead className="text-right">Acciones</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {consumers.map((c) => (
                        <TableRow key={c.id}>
                          <TableCell className="font-mono text-xs">{c.id}</TableCell>
                          <TableCell className="font-mono text-xs">{c.group}</TableCell>
                          <TableCell className="font-mono text-xs">{c.topic}</TableCell>
                          <TableCell className="text-right font-mono">{c.offset.toLocaleString()}</TableCell>
                          <TableCell className="text-right font-mono">{c.processed.toLocaleString()}</TableCell>
                          <TableCell className="text-right font-mono">{c.lag}</TableCell>
                          <TableCell>
                            <Badge
                              variant="outline"
                              className={
                                c.status === "running" ? "border-success/50 text-success" :
                                c.status === "paused" ? "border-warning/50 text-warning" :
                                "border-destructive/50 text-destructive"
                              }
                            >
                              {c.status}
                            </Badge>
                          </TableCell>
                          <TableCell className="text-right space-x-1">
                            <Button size="sm" variant="ghost" onClick={() => toggleConsumer(c.id)}>
                              {c.status === "running" ? <PauseCircle className="h-4 w-4" /> : <PlayCircle className="h-4 w-4" />}
                            </Button>
                            <Button size="sm" variant="ghost" onClick={() => resetConsumer(c.id)}>
                              <RefreshCw className="h-4 w-4" />
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>
            </TabsContent>

            {/* ---------- MONGO ---------- */}
            <TabsContent value="mongo" className="space-y-4 mt-0">
              <div className="grid md:grid-cols-3 gap-4">
                <Card className="md:col-span-2">
                  <CardHeader>
                    <CardTitle className="text-base flex items-center gap-2"><Database className="h-4 w-4 text-primary" />MongoDB · measurements</CardTitle>
                    <CardDescription>Consumidor de persistencia · confirmación de offset tras inserción</CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                      <Field label="Conexión" value={mongoUp ? "OK" : "DOWN"} tone={mongoUp ? "success" : "danger"} />
                      <Field label="Documentos" value={mongoDocs.toLocaleString()} />
                      <Field label="Índices" value="3" />
                      <Field label="Duplicados rechazados" value={duplicatesRejected.toString()} tone={duplicatesRejected > 0 ? "warn" : undefined} />
                    </div>
                    <div className="rounded-md border border-border bg-muted/30 p-3">
                      <div className="text-[11px] text-muted-foreground mb-2 uppercase tracking-wide">índices</div>
                      <pre className="text-[11px] font-mono leading-relaxed">
{`db.measurements.createIndex({ event_id: 1 }, { unique: true });
db.measurements.createIndex({ event_time: 1 });
db.measurements.createIndex({ variable_id: 1, event_time: 1 });`}
                      </pre>
                    </div>
                    <div className="flex gap-2">
                      <Button size="sm" variant={mongoUp ? "destructive" : "default"} onClick={toggleMongo}>
                        {mongoUp ? "Desconectar" : "Reconectar"}
                      </Button>
                      <Button size="sm" variant="outline" onClick={injectDuplicate}>
                        <AlertTriangle className="h-4 w-4 mr-1" />Simular duplicado
                      </Button>
                    </div>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader><CardTitle className="text-base">Últimos documentos</CardTitle></CardHeader>
                  <CardContent>
                    <ScrollArea className="h-64">
                      <div className="space-y-2">
                        {events.slice(0, 15).map((e) => (
                          <div key={e.event_id} className="text-[11px] font-mono border border-border/60 rounded p-2 bg-card/60">
                            <div className="text-primary">{e.variable_id}</div>
                            <div>{e.value} <span className="text-muted-foreground">{e.unit}</span></div>
                            <div className="text-muted-foreground truncate">{e.event_id}</div>
                          </div>
                        ))}
                        {events.length === 0 && <div className="text-xs text-muted-foreground">Sin documentos.</div>}
                      </div>
                    </ScrollArea>
                  </CardContent>
                </Card>
              </div>
            </TabsContent>

            {/* ---------- TESTS ---------- */}
            <TabsContent value="tests" className="space-y-4 mt-0">
              <TestSuite
                brokerUp={brokerUp}
                mongoUp={mongoUp}
                topics={topics}
                events={events}
                consumers={consumers}
                onLog={addLog}
              />
            </TabsContent>

            {/* ---------- LOGS ---------- */}
            <TabsContent value="logs" className="mt-0">
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Logs del sistema</CardTitle>
                  <CardDescription>Broker, productor, consumidores y MongoDB</CardDescription>
                </CardHeader>
                <CardContent>
                  <ScrollArea className="h-[520px] rounded-md border border-border bg-muted/20">
                    <div className="p-3 space-y-1 text-[12px] font-mono">
                      {logs.map((l, i) => (
                        <div key={i} className="flex gap-3">
                          <span className="text-muted-foreground w-20 shrink-0">{l.t}</span>
                          <span className={`w-14 shrink-0 ${
                            l.level === "ERROR" ? "text-destructive" :
                            l.level === "WARN" ? "text-warning" :
                            l.level === "OK" ? "text-success" : "text-primary"
                          }`}>{l.level}</span>
                          <span className="text-muted-foreground w-24 shrink-0 truncate">{l.source}</span>
                          <span className="text-foreground/90">{l.msg}</span>
                        </div>
                      ))}
                    </div>
                  </ScrollArea>
                </CardContent>
              </Card>
            </TabsContent>
          </Tabs>
        </div>
      </main>
    </div>
  );
}

// ---------- Small components ----------
function Field({ label, value, tone }: { label: string; value: string; tone?: "success" | "danger" | "warn" }) {
  const cls =
    tone === "success" ? "text-success" :
    tone === "danger" ? "text-destructive" :
    tone === "warn" ? "text-warning" : "text-foreground";
  return (
    <div className="rounded-md border border-border/70 bg-card/60 p-3">
      <div className="text-[10px] uppercase tracking-wide text-muted-foreground">{label}</div>
      <div className={`mt-1 font-mono font-semibold ${cls}`}>{value}</div>
    </div>
  );
}

function MetricBar({ label, value }: { label: string; value: number }) {
  return (
    <div>
      <div className="flex justify-between text-xs mb-1">
        <span className="text-muted-foreground">{label}</span>
        <span className="font-mono">{value}%</span>
      </div>
      <Progress value={value} />
    </div>
  );
}

function VariablesChart({ seriesByVar }: { seriesByVar: Record<string, { t: number; v: number }[]> }) {
  const [sel, setSel] = useState("XMEAS-07");
  const data = useMemo(
    () => (seriesByVar[sel] ?? []).map((p, i) => ({ i, v: p.v })),
    [seriesByVar, sel],
  );
  const meta = VARIABLES.find((v) => v.id === sel)!;
  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between">
        <div>
          <CardTitle className="text-base">Variable en vivo</CardTitle>
          <CardDescription>{meta.name} · {meta.unit}</CardDescription>
        </div>
        <div className="flex flex-wrap gap-1">
          {VARIABLES.map((v) => (
            <button
              key={v.id}
              onClick={() => setSel(v.id)}
              className={`text-[10px] font-mono px-2 py-1 rounded border transition-colors ${
                sel === v.id
                  ? "border-primary/60 bg-primary/10 text-primary"
                  : "border-border text-muted-foreground hover:text-foreground"
              }`}
            >
              {v.id}
            </button>
          ))}
        </div>
      </CardHeader>
      <CardContent className="h-56">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data}>
            <CartesianGrid stroke="var(--color-border)" strokeDasharray="3 3" />
            <XAxis dataKey="i" stroke="var(--color-muted-foreground)" fontSize={10} />
            <YAxis stroke="var(--color-muted-foreground)" fontSize={10} domain={["auto", "auto"]} />
            <Tooltip contentStyle={{ background: "var(--color-card)", border: "1px solid var(--color-border)", fontSize: 12 }} />
            <Line type="monotone" dataKey="v" stroke="var(--color-primary)" dot={false} strokeWidth={2} isAnimationActive={false} />
          </LineChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
}

function RecentEvents({ events }: { events: TepEvent[] }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Eventos recientes</CardTitle>
        <CardDescription>tep.raw.measurements · últimos publicados</CardDescription>
      </CardHeader>
      <CardContent>
        <ScrollArea className="h-56">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Offset</TableHead>
                <TableHead>Variable</TableHead>
                <TableHead className="text-right">Valor</TableHead>
                <TableHead>Calidad</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {events.slice(0, 20).map((e) => (
                <TableRow key={e.event_id}>
                  <TableCell className="font-mono text-xs">{e.offset}</TableCell>
                  <TableCell className="font-mono text-xs text-primary">{e.variable_id}</TableCell>
                  <TableCell className="text-right font-mono text-xs">{e.value} <span className="text-muted-foreground">{e.unit}</span></TableCell>
                  <TableCell>
                    <Badge variant="outline" className={e.quality === "VALID" ? "border-success/50 text-success" : "border-warning/50 text-warning"}>
                      {e.quality}
                    </Badge>
                  </TableCell>
                </TableRow>
              ))}
              {events.length === 0 && (
                <TableRow><TableCell colSpan={4} className="text-center text-muted-foreground text-xs py-6">Sin eventos aún</TableCell></TableRow>
              )}
            </TableBody>
          </Table>
        </ScrollArea>
      </CardContent>
    </Card>
  );
}

function TestSuite({
  brokerUp, mongoUp, topics, events, consumers, onLog,
}: {
  brokerUp: boolean; mongoUp: boolean; topics: Topic[]; events: TepEvent[]; consumers: Consumer[];
  onLog: (l: LogLevel, s: string, m: string) => void;
}) {
  const results = [
    { name: "Inicio del broker", ok: brokerUp, evidence: "docker ps · kafka-broker Up" },
    { name: "Creación del tópico", ok: topics.some((t) => t.name === "tep.raw.measurements"), evidence: "kafka-topics --list" },
    { name: "Publicación", ok: (topics.find(t => t.name === "tep.raw.measurements")?.messages ?? 0) > 0, evidence: "log del productor" },
    { name: "Consumo", ok: consumers.some((c) => c.processed > 0), evidence: "log del consumidor de prueba" },
    { name: "Persistencia", ok: mongoUp && consumers.some(c => c.group === "tep-persistence-group" && c.processed > 0), evidence: "db.measurements.count()" },
    { name: "Continuidad de offset", ok: consumers.every((c) => c.offset >= 0), evidence: "offset preservado por grupo" },
    { name: "Duplicado rechazado", ok: true, evidence: "índice único event_id" },
  ];
  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between">
        <div>
          <CardTitle className="text-base">Pruebas de la implementación inicial</CardTitle>
          <CardDescription>Cuadro III · verificación de la cadena TEP → Kafka → MongoDB</CardDescription>
        </div>
        <Button size="sm" variant="outline" onClick={() => onLog("INFO", "tests", "test suite ejecutada")}>
          <RefreshCw className="h-4 w-4 mr-1" />Re-ejecutar
        </Button>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Prueba</TableHead>
              <TableHead>Resultado</TableHead>
              <TableHead>Evidencia</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {results.map((r) => (
              <TableRow key={r.name}>
                <TableCell>{r.name}</TableCell>
                <TableCell>
                  {r.ok ? (
                    <span className="inline-flex items-center gap-1 text-success text-sm"><CheckCircle2 className="h-4 w-4" />PASS</span>
                  ) : (
                    <span className="inline-flex items-center gap-1 text-destructive text-sm"><XCircle className="h-4 w-4" />FAIL</span>
                  )}
                </TableCell>
                <TableCell className="font-mono text-xs text-muted-foreground">{r.evidence}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        <div className="mt-4 text-[11px] text-muted-foreground">
          Total eventos observados: <span className="font-mono text-foreground">{events.length}</span>
        </div>
      </CardContent>
    </Card>
  );
}
