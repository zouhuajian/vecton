// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Proto/domain and response-header conversion for metadata services.

use super::filesystem::{FsFailure, FsSuccess, RequestContext};
use beryl_common::error::rpc::{ErrorKind, ProtocolErrorKind, RpcErrorDetail};
use beryl_common::header::{RequestHeader, ResponseHeader};
use beryl_types::{FileBlockLocation, GroupName, GroupStateWatermark, LocatedBlock};
use tracing::Span;

#[allow(clippy::result_large_err)]
pub(crate) fn request_context_from_proto(
    req_header: &Option<beryl_proto::common::RequestHeaderProto>,
) -> Result<RequestContext, RpcErrorDetail> {
    let proto_header = req_header
        .clone()
        .ok_or_else(|| invalid_header_rpc_error("external request requires RequestHeader"))?;
    let caller = RequestHeader::try_from(proto_header)
        .map_err(|err| invalid_header_rpc_error(format!("invalid RequestHeader: {err}")))?;

    Span::current().record("call_id", caller.client.call_id.to_string());
    Span::current().record("client_id", caller.client.client_id.to_string());
    if let Some(ref client_name) = caller.client.client_name {
        Span::current().record("client_name", client_name);
    }
    if let Some(traceparent) = &caller.trace_context.traceparent {
        Span::current().record("traceparent", traceparent);
    }
    if !caller.state.is_empty() {
        Span::current().record("state", format!("{:?}", caller.state));
    }
    Ok(RequestContext {
        route_epoch: req_header.as_ref().and_then(|h| h.route_epoch),
        caller,
    })
}

#[allow(clippy::result_large_err)]
pub(crate) fn extract_and_inject_context(
    req_header: &Option<beryl_proto::common::RequestHeaderProto>,
) -> Result<RequestHeader, RpcErrorDetail> {
    request_context_from_proto(req_header).map(|ctx| ctx.caller)
}

pub(crate) fn invalid_header_rpc_error(message: impl Into<String>) -> RpcErrorDetail {
    RpcErrorDetail::fail(ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader), message)
}

fn error_detail_from_rpc_error(err: &RpcErrorDetail) -> Option<beryl_proto::common::ErrorDetailProto> {
    Some(beryl_proto::convert::rpc_error_to_proto(err))
}

fn build_base_response_header(
    ctx: &RequestContext,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
    state: Vec<GroupStateWatermark>,
) -> beryl_proto::common::ResponseHeaderProto {
    let mut resp_header = ResponseHeader::ok(ctx.caller.client.clone());
    if let Some(group_name) = group_name {
        resp_header = resp_header.with_group_name(group_name);
    }
    resp_header.state = state;
    let mut proto_header: beryl_proto::common::ResponseHeaderProto = (&resp_header).into();
    if let Some(epoch) = mount_epoch {
        proto_header.mount_epoch = Some(epoch);
    }
    if let Some(epoch) = route_epoch {
        proto_header.route_epoch = Some(epoch);
    }
    proto_header
}

fn client_from_request_header(
    req_header: &Option<beryl_proto::common::RequestHeaderProto>,
) -> Option<beryl_common::header::ClientInfo> {
    req_header
        .as_ref()
        .and_then(|header| header.client.clone())
        .and_then(|client| beryl_common::header::ClientInfo::try_from(client).ok())
}

pub(crate) fn ok_header_from_fs_success<T>(
    ctx: &RequestContext,
    success: &FsSuccess<T>,
) -> beryl_proto::common::ResponseHeaderProto {
    build_base_response_header(
        ctx,
        success.group_name.clone(),
        success.mount_epoch,
        success.route_epoch,
        success.state.clone(),
    )
}

pub(crate) fn header_from_fs_failure(
    ctx: &RequestContext,
    failure: &FsFailure,
) -> beryl_proto::common::ResponseHeaderProto {
    let mut header = build_base_response_header(
        ctx,
        failure.group_name.clone(),
        failure.mount_epoch,
        failure.route_epoch,
        failure.state.clone(),
    );
    header.error = error_detail_from_rpc_error(&failure.error);
    header
}

pub(crate) fn ok_header_from_request(
    req_header: &Option<beryl_proto::common::RequestHeaderProto>,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
) -> beryl_proto::common::ResponseHeaderProto {
    let mut header: beryl_proto::common::ResponseHeaderProto = client_from_request_header(req_header)
        .map(|client| {
            let mut header = ResponseHeader::ok(client);
            if let Some(group_name) = group_name.clone() {
                header = header.with_group_name(group_name);
            }
            (&header).into()
        })
        .unwrap_or_default();
    if let Some(group_name) = group_name {
        header.group_name = group_name.to_string();
    }
    header.mount_epoch = mount_epoch;
    header
}

pub fn header_from_rpc_error(
    req_header: &Option<beryl_proto::common::RequestHeaderProto>,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    err: &RpcErrorDetail,
) -> beryl_proto::common::ResponseHeaderProto {
    let mut header: beryl_proto::common::ResponseHeaderProto = client_from_request_header(req_header)
        .map(|client| {
            let mut header = ResponseHeader::from_rpc_error(client, err.clone());
            if let Some(group_name) = group_name.clone() {
                header = header.with_group_name(group_name);
            }
            (&header).into()
        })
        .unwrap_or_default();
    if let Some(group_name) = group_name {
        header.group_name = group_name.to_string();
    }
    header.mount_epoch = mount_epoch;
    header.error = error_detail_from_rpc_error(err);
    header
}

pub(crate) fn located_block_to_proto(target: &LocatedBlock) -> beryl_proto::metadata::LocatedBlockProto {
    target.into()
}

pub(crate) fn location_to_proto(location: &FileBlockLocation) -> beryl_proto::metadata::FileBlockLocationProto {
    location.into()
}
