import jwt, { type JwtPayload, type SignOptions } from "jsonwebtoken";
import type { UserRole } from "@microgrid/shared";

interface AuthUser {
  username: string;
  role: UserRole;
}

interface LoginResult {
  accessToken: string;
  expiresIn: number;
  role: UserRole;
}

const roleSet = new Set<UserRole>(["viewer", "operator", "admin"]);

function requiredEnv(name: string) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }

  return value;
}

function isUserRole(value: unknown): value is UserRole {
  return typeof value === "string" && roleSet.has(value as UserRole);
}

function getConfiguredUsers(): Array<AuthUser & { password: string }> {
  return [
    {
      username: requiredEnv("AUTH_ADMIN_USERNAME"),
      password: requiredEnv("AUTH_ADMIN_PASSWORD"),
      role: "admin"
    },
    {
      username: requiredEnv("AUTH_OPERATOR_USERNAME"),
      password: requiredEnv("AUTH_OPERATOR_PASSWORD"),
      role: "operator"
    },
    {
      username: requiredEnv("AUTH_VIEWER_USERNAME"),
      password: requiredEnv("AUTH_VIEWER_PASSWORD"),
      role: "viewer"
    }
  ];
}

function getJwtSecret() {
  return requiredEnv("JWT_SECRET");
}

export function authenticateUser(username: string, password: string): AuthUser | null {
  const matchedUser = getConfiguredUsers().find((user) => user.username === username && user.password === password);
  if (!matchedUser) {
    return null;
  }

  return {
    username: matchedUser.username,
    role: matchedUser.role
  };
}

export function issueLoginToken(user: AuthUser): LoginResult {
  const secret = getJwtSecret();
  const expiresInConfig = (process.env.JWT_EXPIRES_IN ?? "1h") as SignOptions["expiresIn"];

  const accessToken = jwt.sign({ role: user.role }, secret, {
    subject: user.username,
    expiresIn: expiresInConfig
  });

  const decoded = jwt.decode(accessToken);
  const decodedPayload = typeof decoded === "string" || !decoded ? null : (decoded as JwtPayload);
  const issuedAt = typeof decodedPayload?.iat === "number" ? decodedPayload.iat : Math.floor(Date.now() / 1000);
  const expiresAt = typeof decodedPayload?.exp === "number" ? decodedPayload.exp : issuedAt + 3600;

  return {
    accessToken,
    expiresIn: Math.max(1, expiresAt - issuedAt),
    role: user.role
  };
}

export function verifyAccessToken(token: string): AuthUser | null {
  try {
    const payload = jwt.verify(token, getJwtSecret());
    if (!payload || typeof payload === "string") {
      return null;
    }

    const subject = payload.sub;
    const role = payload.role;

    if (typeof subject !== "string" || !isUserRole(role)) {
      return null;
    }

    return {
      username: subject,
      role
    };
  } catch {
    return null;
  }
}
