//
//  PhDownloadState.swift
//  PhDownloader Example
//
//  Created by Petrus on 7/4/20.
//  Copyright © 2020 Petrus Nguyễn Thái Học. All rights reserved.
//

import Foundation

public enum PhDownloadState: CustomStringConvertible, Hashable {
  case undefined

  case enqueued

  case downloading(progress: Int)

  case completed

  case failed

  case cancelled

  public var description: String {
    switch self {
    case .undefined:
      return "undefined"
    case .enqueued:
      return "enqueue"
    case .downloading(let progress):
      return "downloading(\(progress)%)"
    case .completed:
      return "completed"
    case .failed:
      return "failed"
    case .cancelled:
      return "cancelled"
    }
  }
}
