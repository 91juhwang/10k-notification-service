import type { NextFunction, Request, Response } from "express";
import type { UserRole } from "@microgrid/shared";
import { verifyAccessToken } from "../services/auth-service.js";

export interface AuthContext {
  username: string;
  role: UserRole;
}

type ResponseLocals = {
  auth?: AuthContext;
};

function getBearerToken(req: Request) {
  const authHeader = req.header("authorization");
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return null;
  }

  const token = authHeader.slice("Bearer ".length).trim();
  return token.length > 0 ? token : null;
}

export function getAuthContext(res: Response) {
  return (res.locals as ResponseLocals).auth;
}

export function requireBearerAuth(req: Request, res: Response, next: NextFunction) {
  const token = getBearerToken(req);
  if (!token) {
    res.status(401).json({ message: "Unauthorized" });
    return;
  }

  const authContext = verifyAccessToken(token);
  if (!authContext) {
    res.status(401).json({ message: "Unauthorized" });
    return;
  }

  (res.locals as ResponseLocals).auth = authContext;
  next();
}

export function requireRoles(allowedRoles: UserRole[]) {
  return (_req: Request, res: Response, next: NextFunction) => {
    const authContext = getAuthContext(res);
    if (!authContext || !allowedRoles.includes(authContext.role)) {
      res.status(401).json({ message: "Unauthorized" });
      return;
    }

    next();
  };
}
