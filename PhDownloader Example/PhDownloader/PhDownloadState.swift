//
//  PhDownloadState.swift
//  PhDownloader Example
//
//  Created by Petrus on 7/4/20.
//  Copyright © 2020 Petrus Nguyễn Thái Học. All rights reserved.
//

import Foundation

public enum PhDownloadState: CustomDebugStringConvertible, Hashable {
  case undefined

  case enqueued

  case downloading(bytesWritten: Int64, totalBytes: Int64, percentage: Int)

  case completed

  case failed

  case cancelled

  public var debugDescription: String {
    switch self {
    case .undefined:
      return "undefined"
    case .enqueued:
      return "enqueue"
    case .downloading(let bytesWritten, let totalBytes, let percentage):
      return "downloading: \(percentage)% bytesWritten=\(bytesWritten), totalBytes=\(totalBytes)"
    case .completed:
      return "completed"
    case .failed:
      return "failed"
    case .cancelled:
      return "cancelled"
    }
  }
}
