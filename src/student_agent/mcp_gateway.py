from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import httpx2
from jsonschema import Draft202012Validator, FormatChecker

from .contracts import Contracts

if TYPE_CHECKING:
    from mcp import ClientSession


def is_transient_mcp_error(exc: BaseException) -> bool:
    pending = [exc]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        text = f'{type(current).__name__} {current}'.lower()
        if isinstance(current, (TimeoutError, ConnectionError)) or any(
            token in text
            for token in (
                'timeout', 'connect', 'transport', 'temporar', 'unavailable',
                'readerror', 'connection closed',
            )
        ):
            return True
        pending.extend(getattr(current, 'exceptions', ()))
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
    return False


class EvidenceGateway:
    def __init__(self, session: ClientSession, contracts: Contracts) -> None:
        self._session = session
        self._contracts = contracts
        self._specs: dict[str, dict[str, Any]] | None = None
        self._evidence_scope: dict[str, tuple[str, str]] = {}

    async def list_tools(self) -> list[str]:
        return sorted(await self.tool_specs())

    async def tool_specs(self) -> dict[str, dict[str, Any]]:
        if self._specs is not None:
            return self._specs
        response = await self._session.list_tools()
        self._specs = {
            tool.name: {
                'description': getattr(tool, 'description', None) or '',
                'input_schema': (
                    getattr(tool, 'inputSchema', None)
                    or getattr(tool, 'input_schema', None)
                    or {}
                ),
                'output_schema': (
                    getattr(tool, 'outputSchema', None)
                    or getattr(tool, 'output_schema', None)
                    or {}
                ),
            }
            for tool in response.tools
        }
        return self._specs

    async def call(self, tool_name: str, *, case_id: str, **arguments: Any) -> dict[str, Any]:
        if not isinstance(case_id, str) or not case_id:
            raise ValueError('MCP call requires a non-empty case_id')
        specs = await self.tool_specs()
        if tool_name not in specs:
            raise ValueError(f'MCP tool was not discovered: {tool_name}')
        payload = {'case_id': case_id, **arguments}
        schema = specs[tool_name]['input_schema']
        errors = sorted(
            Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(payload),
            key=lambda error: tuple(map(str, error.absolute_path)),
        )
        if errors:
            error = errors[0]
            location = '.'.join(map(str, error.absolute_path)) or '$'
            raise ValueError(f'MCP tool {tool_name} input:{location}: {error.message}')
        result = await self._session.call_tool(tool_name, arguments=payload)
        if getattr(result, 'is_error', False) or getattr(result, 'isError', False):
            message = ' '.join(
                block.text for block in result.content if getattr(block, 'text', None)
            )
            detail = message or 'unknown error'
            raise RuntimeError(f'MCP tool {tool_name} failed: {detail}')
        evidence = getattr(result, 'structuredContent', None)
        if evidence is None:
            evidence = getattr(result, 'structured_content', None)
        if evidence is None:
            text_blocks = [
                block.text for block in result.content if getattr(block, 'text', None)
            ]
            if len(text_blocks) != 1:
                raise ValueError(f'MCP tool {tool_name} did not return one evidence object')
            evidence = json.loads(text_blocks[0])
        self._contracts.validate_evidence(evidence, f'MCP tool {tool_name}')
        evidence_ref = evidence['evidence_ref']
        scope = (case_id, evidence['result_hash'])
        previous = self._evidence_scope.get(evidence_ref)
        if previous is not None and previous != scope:
            raise ValueError('MCP evidence_ref was reused across case or result scope')
        self._evidence_scope[evidence_ref] = scope
        return evidence

    def assert_evidence_scope(self, case_id: str, evidence_refs: Iterable[str]) -> None:
        for evidence_ref in evidence_refs:
            scope = self._evidence_scope.get(evidence_ref)
            if scope is None:
                raise ValueError(f'unknown MCP evidence_ref: {evidence_ref}')
            if scope[0] != case_id:
                raise ValueError(f'cross-case MCP evidence_ref: {evidence_ref}')


@asynccontextmanager
async def connect_gateway(
    endpoint: str, team_api_key: str, contracts: Contracts
) -> AsyncIterator[EvidenceGateway]:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    headers = {'Authorization': f'Bearer {team_api_key}'}
    timeout = httpx2.Timeout(300.0, connect=30.0, write=30.0, pool=30.0)
    async with (
        httpx2.AsyncClient(headers=headers, timeout=timeout) as http_client,
        streamable_http_client(endpoint, http_client=http_client) as (read_stream, write_stream),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        yield EvidenceGateway(session, contracts)
