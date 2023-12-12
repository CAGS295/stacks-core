import { tx } from "@hirosystems/clarinet-sdk";
import { Cl } from "@stacks/transactions";
import { describe, expect, it } from "vitest";

const accounts = simnet.getAccounts();
const alice = accounts.get("wallet_1")!;
const bob = accounts.get("wallet_2")!;

const ERR_DELEGATION_ALREADY_REVOKED = 33;

describe("test pox revoke-delegate-stx", () => {
  it("should return last delegation state", () => {
    // delegate and revoke
    let result = simnet.mineBlock([
      tx.callPublicFn(
        "pox-4",
        "delegate-stx",
        [Cl.uint(10000), Cl.standardPrincipal(bob), Cl.none(), Cl.none()],
        alice
      ),
      tx.callPublicFn("pox-4", "revoke-delegate-stx", [], alice),
    ]);
    expect(result[0].result).toBeOk(Cl.bool(true));
    expect(result[1].result).toBeOk(
      Cl.some(
        Cl.tuple({
          "delegated-to": Cl.standardPrincipal(bob),
          "amount-ustx": Cl.uint(10000),
          "pox-addr": Cl.none(),
          "until-burn-ht": Cl.none(),
        })
      )
    );
  });

  it("should return fail for second revoke call", () => {
    // delegate and revoke
    let result = simnet.mineBlock([
      tx.callPublicFn(
        "pox-4",
        "delegate-stx",
        [Cl.uint(10000), Cl.standardPrincipal(bob), Cl.none(), Cl.none()],
        alice
      ),
      tx.callPublicFn("pox-4", "revoke-delegate-stx", [], alice),
    ]);
    expect(result[0].result).toBeOk(Cl.bool(true));
    expect(result[1].result).toBeOk(
      Cl.some(
        Cl.tuple({
          "delegated-to": Cl.standardPrincipal(bob),
          "amount-ustx": Cl.uint(10000),
          "pox-addr": Cl.none(),
          "until-burn-ht": Cl.none(),
        })
      )
    );
    // revoke again
    result = simnet.mineBlock([
      tx.callPublicFn("pox-4", "revoke-delegate-stx", [], alice),
    ]);

    expect(result[0].result).toBeErr(Cl.int(ERR_DELEGATION_ALREADY_REVOKED));
  });
});
