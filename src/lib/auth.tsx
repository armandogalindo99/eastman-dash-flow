import { createContext, useContext, useEffect, useState, type ReactNode } from "react";

export type Role = "admin" | "operador";

export type SessionUser = { username: string; role: Role };

const USERS: { username: string; password: string; role: Role }[] = [
  { username: "Admin", password: "Qwerty123", role: "admin" },
  { username: "Eduard", password: "Eduardo123", role: "operador" },
  { username: "Daniel", password: "Daniel123", role: "operador" },
  { username: "Armnad", password: "Armand123", role: "operador" },
];

const STORAGE_KEY = "tep-kafka-session";

type AuthCtx = {
  user: SessionUser | null;
  ready: boolean;
  signIn: (username: string, password: string) => { ok: boolean; error?: string };
  signOut: () => void;
};

const Ctx = createContext<AuthCtx | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<SessionUser | null>(null);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    try {
      const raw = sessionStorage.getItem(STORAGE_KEY);
      if (raw) setUser(JSON.parse(raw) as SessionUser);
    } catch {
      /* ignore */
    }
    setReady(true);
  }, []);

  const signIn = (username: string, password: string) => {
    const u = USERS.find(
      (x) => x.username.toLowerCase() === username.trim().toLowerCase() && x.password === password,
    );
    if (!u) return { ok: false, error: "Usuario o contraseña incorrectos." };
    const session: SessionUser = { username: u.username, role: u.role };
    setUser(session);
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(session));
    } catch {
      /* ignore */
    }
    return { ok: true };
  };

  const signOut = () => {
    setUser(null);
    try {
      sessionStorage.removeItem(STORAGE_KEY);
    } catch {
      /* ignore */
    }
  };

  return <Ctx.Provider value={{ user, ready, signIn, signOut }}>{children}</Ctx.Provider>;
}

export function useAuth() {
  const ctx = useContext(Ctx);
  if (!ctx) throw new Error("useAuth debe usarse dentro de AuthProvider");
  return ctx;
}
